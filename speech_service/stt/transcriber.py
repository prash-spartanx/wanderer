import whisper 
import wave
import os
import logging
import time
import sounddevice as sd
import config

class AudioTranscriber:
    def __init__(self, model_size: str = "base"):
        logging.info("Initializing AudioTranscriber...")
        self.model = whisper.load_model(model_size)
        logging.info("AudioTranscriber initialized.")

    def record_audio(self, duration: int = 5, output_filename: str = "temp_command.wav") -> str | None:
        wf = None
        try:
            logging.info(f"Recording audio for {duration} seconds...")
            sample_rate = 16000
            audio = sd.rec(
                int(sample_rate * duration),
                samplerate=sample_rate,
                channels=1,
                dtype="int16",
                device=config.AUDIO_INPUT_DEVICE,
            )
            sd.wait()

            wf = wave.open(output_filename, "wb")
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(sample_rate)
            wf.writeframes(audio.tobytes())
            
            logging.info(f"Audio recorded and saved to {output_filename}")
            return output_filename
            
        except Exception as e:
            logging.error(f"Error occurred while recording audio: {e}")
            return None
        finally:
            # Guarantee hardware locks are released
            if wf:
                wf.close()
        
    def transcribe(self, audio_file_path: str) -> str | None:
        try:
            logging.info(f"Transcribing audio from {audio_file_path}...")
            result = self.model.transcribe(audio_file_path)
            logging.info(f"Transcription completed.")
            return result["text"].strip()
        except Exception as e:
            logging.error(f"Error occurred while transcribing audio: {e}")
            return None
        finally:
            # Guarantee disk cleanup
            if os.path.exists(audio_file_path):
                for attempt in range(3):
                    try:
                        os.remove(audio_file_path)
                        break
                    except PermissionError:
                        if attempt == 2:
                            logging.warning(
                                "Could not remove temporary audio file yet: %s",
                                audio_file_path,
                            )
                        else:
                            time.sleep(0.2)