from typing import Optional

from pipecat.audio.utils import create_stream_resampler
from pipecat.frames.frames import AudioRawFrame, CancelFrame, EndFrame, Frame, InputAudioRawFrame, StartFrame
from pipecat.serializers.base_serializer import FrameSerializer, FrameSerializerType
from pydantic import BaseModel


class AsteriskSerializer(FrameSerializer):

    class InputParams(BaseModel):

        sample_rate: int = 8000
        asterisk_in_sample_rate: int = 16000
        asterisk_out_sample_rate: int = 8000

    
    def __init__(self, params: Optional[InputParams] = None, channel_id: str = None, ari_url: str = None, ari_username: str = None, ari_password: str = None):
    
        self._params = params or AsteriskSerializer.InputParams()

        self._channel_id = channel_id
        self._ari_url = ari_url
        self._ari_username = ari_username
        self._ari_password = ari_password

        self._in_sample_rate = self._params.asterisk_in_sample_rate
        self._out_sample_rate = self._params.asterisk_out_sample_rate

        self._sample_rate = 0

        self._input_resampler = create_stream_resampler()
        self._output_resampler = create_stream_resampler()

    @property
    def type(self) -> FrameSerializerType:
        """Get the serialization type supported by this serializer.

        Returns:
            The FrameSerializerType indicating binary or text format.
        """
        return FrameSerializerType.BINARY
    
    async def setup(self, frame: StartFrame):
        """Initialize the serializer with startup configuration.

        Args:
            frame: StartFrame containing initialization parameters.
        """
        self._sample_rate = frame.audio_in_sample_rate or self._params.sample_rate

    async def serialize(self, frame: Frame) -> str | bytes | None:

        if isinstance(frame, (EndFrame, CancelFrame)):
            self._hang_up()
            return None

        if isinstance(frame, AudioRawFrame):

            try:

                resampled_data = await self._output_resampler.resample(
                    frame.audio,
                    input_rate=self._sample_rate,
                    output_rate=self._out_sample_rate,
                )

                return resampled_data
            
            except Exception as e:
                print(f"Error in AsteriskSerializer serialize: {e}")
                return None
        
        return None

    async def _hang_up(self):
        try:
            import aiohttp

            channel_id = self._channel_id
            ari_url = self._ari_url
            ari_username = self._ari_username
            ari_password = self._ari_password   

            # Asterisk Hangup Endpoint
            hangup_url = f"{ari_url}/channels/{channel_id}"

            # Basic Auth
            auth = aiohttp.BasicAuth(ari_username, ari_password)

            # Make Delete Request to Hangup Channel
            async with aiohttp.ClientSession() as session:
                async with session.delete(hangup_url, auth=auth) as response:

                    if response.status == 204:
                        return True
                    
                    else:
                        return False
                    
        except Exception as e:
            print(f"Error in AsteriskSerializer _hang_up: {e}")
            return False

    async def deserialize(self, data: str | bytes) -> str | bytes | None:

        if not isinstance(data, (bytes, bytearray)):
            return None
        
        try:

            resampled_data = await self._input_resampler.resample(
                data,
                input_rate=self._sample_rate,
                output_rate=self._in_sample_rate,
            )

            return InputAudioRawFrame(
                audio=resampled_data,
                sample_rate=self._in_sample_rate,
                num_channels=1,
            )
            
        except Exception as e:
            print(f"Error in AsteriskSerializer deserialize: {e}")
            return None