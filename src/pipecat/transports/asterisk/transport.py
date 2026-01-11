import asyncio
import socket
import struct
import time
from typing import Awaitable, Callable, Optional

from pydantic import BaseModel
from pipecat.frames.frames import CancelFrame, EndFrame, Frame, InputAudioRawFrame, InterruptionFrame, OutputAudioRawFrame, OutputTransportMessageFrame, OutputTransportMessageUrgentFrame, StartFrame
from pipecat.processors.frame_processor import FrameDirection
from pipecat.serializers.base_serializer import FrameSerializer
from pipecat.transports.base_input import BaseInputTransport
from pipecat.transports.base_output import BaseOutputTransport
from pipecat.transports.base_transport import BaseTransport, TransportParams

class HeaderType:
    MSG_UUID = 0x01
    MSG_AUDIO = 0x10
    MSG_HANGUP = 0x00
    MSG_DTMF = 0x03
    MSG_ERROR = 0xff

class AsteriskTransportParams(TransportParams):

    serializer: Optional[FrameSerializer] = None
    session_timeout: Optional[int] = None


class AsteriskTransportCallbacks(BaseModel):
    on_client_connected: Callable[[str], Awaitable[None]]
    on_client_disconnected: Callable[[str], Awaitable[None]]
    on_session_timeout: Callable[[str], Awaitable[None]]
    on_websocket_ready: Callable[[], Awaitable[None]]

class AsteriskInputTransport(BaseInputTransport):
    def __init__(self, transport: BaseTransport, host: str, port: int, params: AsteriskTransportParams, callbacks: AsteriskTransportCallbacks, name: Optional[str] = None):
        # FIXED: Added name parameter and proper super().__init__() call
        super().__init__(params, name=name)
        self._transport = transport
        self._host = host  
        self._port = port  
        self._params = params
        self._callbacks = callbacks

        self._client_writer: asyncio.StreamWriter | None = None

        self._server_task = None

        self._monitor_task = None

        self._stop_server_event = asyncio.Event()

        self._initialized = False

    async def start(self, frame: StartFrame):
        await super().start(frame)

        if self._initialized:
            return

        self._initialized = True

        if self._params.serializer:
            await self._params.serializer.setup(frame)
        
        if not self._server_task:
            self._server_task = self.create_task(self._server_task_handler())

        await self.set_transport_ready(frame)

    async def stop(self, frame: EndFrame):
        """Stop the WebSocket server and cleanup resources.

        Args:
            frame: The end frame signaling transport shutdown.
        """
        await super().stop(frame)
        self._stop_server_event.set()
        if self._monitor_task:
            await self.cancel_task(self._monitor_task)
            self._monitor_task = None
        if self._server_task:
            await self._server_task
            self._server_task = None

    async def cancel(self, frame: CancelFrame):
        """Cancel the WebSocket server and stop all processing.

        Args:
            frame: The cancel frame signaling immediate cancellation.
        """
        await super().cancel(frame)
        if self._monitor_task:
            await self.cancel_task(self._monitor_task)
            self._monitor_task = None
        if self._server_task:
            await self.cancel_task(self._server_task)
            self._server_task = None

    async def cleanup(self):
        """Cleanup resources and parent transport."""
        await super().cleanup()
        await self._transport.cleanup()

    async def _server_task_handler(self):
        """Handle WebSocket server startup and client connections."""
        # FIXED: Use stored host and port instead of reader/writer
        print(f"Starting server on {self._host}:{self._port}")
        server = await asyncio.start_server(
            self._client_handler, 
            host=self._host,
            port=self._port
        )
        await self._callbacks.on_websocket_ready()
        async with server:
            await self._stop_server_event.wait()

    async def _client_handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle individual client connections and message processing."""
        peer = writer.get_extra_info("peername")
        print(f"New client connection from {peer}")
        if self._client_writer:
            self._client_writer.close()
            await self._client_writer.wait_closed()
            print("Only one client connected, using new connection")

        self._client_writer = writer

        await self._callbacks.on_client_connected(str(peer))

        if not self._monitor_task and self._params.session_timeout:
            self._monitor_task = self.create_task(
                self._monitor_websocket(self._params.session_timeout)
            )

        try:
            while True:
                header = await reader.readexactly(3)
                msg_type = header[0]
                length = struct.unpack(">H", header[1:])[0]
                payload = await reader.readexactly(length)
        
                if not self._params.serializer:
                    continue

                # FIXED: Initialize frame variable
                frame = None

                if msg_type == HeaderType.MSG_AUDIO:
                    frame = await self._params.serializer.deserialize(payload)

                elif msg_type == HeaderType.MSG_HANGUP:
                    # FIXED: Pass correct argument (peer string, not writer)
                    await self._callbacks.on_client_disconnected(str(peer))
                    break

                # elif msg_type == HeaderType.MSG_DTMF:
                #     frame = await self._params.serializer.deserialize(payload)

                elif msg_type == HeaderType.MSG_ERROR:
                    print(f"Received error message from client {peer}")
                    break

                else:
                    print(f"Unknown message type: {msg_type}")
                    continue

                if not frame:
                    continue

                if isinstance(frame, InputAudioRawFrame):
                    await self.push_audio_frame(frame)
                else:
                    await self.push_frame(frame)

        except Exception as e:
            print(f"{self} exception receiving data: {e.__class__.__name__} ({e})")

        # Notify disconnection
        # FIXED: Pass peer string, not writer
        await self._callbacks.on_client_disconnected(str(peer))

        # FIXED: Use writer variable, not self._client_writer
        writer.close()
        await writer.wait_closed()
        self._client_writer = None

        print(f"Client {peer} disconnected")

    async def _monitor_websocket(
        self, session_timeout: int
    ):
        """Monitor WebSocket connection for session timeout."""
        try:
            await asyncio.sleep(session_timeout)
            if not self._client_writer:
                await self._callbacks.on_session_timeout("session-timeout")
        except asyncio.CancelledError:
            print(f"Monitoring task cancelled for: {self._client_writer}")
            raise

class AsteriskOutputTransport(BaseOutputTransport):
    def __init__(self, transport: BaseTransport, params: AsteriskTransportParams, name: Optional[str] = None, **kwargs):
        """Initialize the WebSocket server output transport.

        Args:
            transport: The parent transport instance.
            params: WebSocket server configuration parameters.
            name: Optional name for the output processor.
            **kwargs: Additional arguments passed to parent class.
        """
        # FIXED: Pass name parameter to super().__init__()
        super().__init__(params, name=name, **kwargs)

        self._transport = transport
        self._params = params

        self._client_writer: asyncio.StreamWriter | None = None

        # write_audio_frame() is called quickly, as soon as we get audio
        # (e.g. from the TTS), and since this is just a network connection we
        # would be sending it to quickly. Instead, we want to block to emulate
        # an audio device, this is what the send interval is. It will be
        # computed on StartFrame.
        self._send_interval = 0
        self._next_send_time = 0

        # Whether we have seen a StartFrame already.
        self._initialized = False

    async def set_client_connection(self, writer: asyncio.StreamWriter | None):
        """Set the active client WebSocket connection.

        Args:
            writer: The StreamWriter connection to set as active, or None to clear.
        """
        if self._client_writer:
            self._client_writer.close()
            await self._client_writer.wait_closed()
            print("Only one client allowed, using new connection")
        self._client_writer = writer

    async def start(self, frame: StartFrame):
        """Start the output transport and initialize components.

        Args:
            frame: The start frame containing initialization parameters.
        """
        await super().start(frame)

        if self._initialized:
            return

        self._initialized = True

        if self._params.serializer:
            await self._params.serializer.setup(frame)
        self._send_interval = (self.audio_chunk_size / self.sample_rate) / 2
        await self.set_transport_ready(frame)

    async def stop(self, frame: EndFrame):
        """Stop the output transport and send final frame.

        Args:
            frame: The end frame signaling transport shutdown.
        """
        await super().stop(frame)
        await self._write_frame(frame)

    async def cancel(self, frame: CancelFrame):
        """Cancel the output transport and send cancellation frame.

        Args:
            frame: The cancel frame signaling immediate cancellation.
        """
        await super().cancel(frame)
        await self._write_frame(frame)

    async def cleanup(self):
        """Cleanup resources and parent transport."""
        await super().cleanup()
        await self._transport.cleanup()

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        """Process frames and handle interruption timing.

        Args:
            frame: The frame to process.
            direction: The direction of frame flow in the pipeline.
        """
        await super().process_frame(frame, direction)

        if isinstance(frame, InterruptionFrame):
            await self._write_frame(frame)
            self._next_send_time = 0

    async def send_message(
        self, frame: OutputTransportMessageFrame | OutputTransportMessageUrgentFrame
    ):
        """Send a transport message frame to the client.

        Args:
            frame: The transport message frame to send.
        """
        await self._write_frame(frame)

    async def write_audio_frame(self, frame: OutputAudioRawFrame) -> bool:
        """Write an audio frame to the WebSocket client with timing control.

        Args:
            frame: The output audio frame to write.

        Returns:
            True if the audio frame was written successfully, False otherwise.
        """
        if not self._client_writer:
            return False

        frame = OutputAudioRawFrame(
            audio=frame.audio,
            sample_rate=self.sample_rate,
            num_channels=self._params.audio_out_channels,
        )

        await self._write_frame(frame)

        # Simulate audio playback with a sleep.
        await self._write_audio_sleep()

        return True
    
    async def _write_frame(self, frame: Frame):
        """Serialize and send a frame to the WebSocket client."""
        if not self._params.serializer:
            return

        try:
            payload = await self._params.serializer.serialize(frame)
            if payload and self._client_writer:
                header = struct.pack("B", HeaderType.MSG_AUDIO) + struct.pack(">H", len(payload))
                self._client_writer.write(header + payload)
                await self._client_writer.drain()
        except Exception as e:
            print(f"{self} exception sending data: {e.__class__.__name__} ({e})")

    async def _write_audio_sleep(self):
        """Simulate audio device timing by sleeping between audio chunks."""
        # Simulate a clock.
        current_time = time.monotonic()
        sleep_duration = max(0, self._next_send_time - current_time)
        await asyncio.sleep(sleep_duration)
        if sleep_duration == 0:
            self._next_send_time = time.monotonic() + self._send_interval
        else:
            self._next_send_time += self._send_interval


class AsteriskTransport(BaseTransport):
    def __init__(
        self,
        params: AsteriskTransportParams,
        host: str = "localhost",
        port: int = 8765,
        input_name: Optional[str] = None,
        output_name: Optional[str] = None,
    ):
        """Initialize the WebSocket server transport.

        Args:
            params: WebSocket server configuration parameters.
            host: Host address to bind the server to. Defaults to "localhost".
            port: Port number to bind the server to. Defaults to 8765.
            input_name: Optional name for the input processor.
            output_name: Optional name for the output processor.
        """
        super().__init__(input_name=input_name, output_name=output_name)
        self._host = host
        self._port = port
        self._params = params

        self._callbacks = AsteriskTransportCallbacks(
            on_client_connected=self._on_client_connected,
            on_client_disconnected=self._on_client_disconnected,
            on_session_timeout=self._on_session_timeout,
            on_websocket_ready=self._on_websocket_ready,
        )
        self._input: Optional[AsteriskInputTransport] = None
        self._output: Optional[AsteriskOutputTransport] = None

        self._client_writer: asyncio.StreamWriter | None = None

        # Register supported handlers. The user will only be able to register
        # these handlers.
        self._register_event_handler("on_client_connected")
        self._register_event_handler("on_client_disconnected")
        self._register_event_handler("on_session_timeout")
        self._register_event_handler("on_websocket_ready")

    def input(self) -> AsteriskInputTransport:
        """Get the input transport for receiving client data.

        Returns:
            The WebSocket server input transport instance.
        """
        if not self._input:
            self._input = AsteriskInputTransport(
                self, self._host, self._port, self._params, self._callbacks, name=self._input_name
            )
        return self._input

    def output(self) -> AsteriskOutputTransport:
        """Get the output transport for sending data to clients.

        Returns:
            The WebSocket server output transport instance.
        """
        if not self._output:
            self._output = AsteriskOutputTransport(
                self, self._params, name=self._output_name
            )
        return self._output

    async def _on_client_connected(self, websocket):
        """Handle client connection events."""
        if self._output:
            await self._output.set_client_connection(websocket)
            await self._call_event_handler("on_client_connected", websocket)
        else:
            print("A WebsocketServerTransport output is missing in the pipeline")

    async def _on_client_disconnected(self, websocket):
        """Handle client disconnection events."""
        if self._output:
            await self._output.set_client_connection(None)
            await self._call_event_handler("on_client_disconnected", websocket)
        else:
            print("A WebsocketServerTransport output is missing in the pipeline")

    async def _on_session_timeout(self, websocket):
        """Handle client session timeout events."""
        await self._call_event_handler("on_session_timeout", websocket)

    async def _on_websocket_ready(self):
        """Handle WebSocket server ready events."""
        await self._call_event_handler("on_websocket_ready")