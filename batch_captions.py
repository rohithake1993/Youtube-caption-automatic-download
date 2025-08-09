import os
import re
import time
import logging
from urllib import parse
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
from pytube import Playlist, YouTube
from tqdm import tqdm
from googletrans import Translator

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

CAPTIONS_DIR = "captions"
BATCH_SIZE = 20
DELAY_BETWEEN_BATCHES = 5  # seconds
DELAY_BETWEEN_VIDEOS = 1   # seconds, to avoid rate limiting

translator = Translator()

def sanitize_filename(name):
    """Remove invalid characters from a string to make it a valid filename."""
    return re.sub(r'[\\/*?:"<>|]', "", name)

def read_batches_from_links_file(filepath):
    """Reads batches of links from a file, separated by '-----'."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        logging.error(f"Links file not found at '{filepath}'")
        return []
    # Normalize separator and split into batches
    content = re.sub(r'-{5,}', '-------------', content)
    batches = [batch.strip().splitlines() for batch in content.split('-------------')]
    return [[link.strip() for link in batch if link.strip()] for batch in batches if batch]

def read_prompt(filepath):
    """Reads an optional prompt from a file."""
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read().strip()
    return ""

def fetch_video_ids_from_playlist(playlist_url):
    """Expands a YouTube playlist URL into a list of video IDs."""
    try:
        playlist = Playlist(playlist_url)
        # Accessing video_urls property initializes the list
        if not playlist.video_urls:
            logging.warning(f"No videos found or playlist is private: {playlist_url}")
            return []
        video_ids = [parse.parse_qs(parse.urlparse(url).query)['v'][0] for url in playlist.video_urls if 'v' in parse.parse_qs(parse.urlparse(url).query)]
        logging.info(f"Playlist '{playlist.title}' expanded with {len(video_ids)} videos.")
        return video_ids
    except Exception as e:
        logging.error(f"Failed to expand playlist {playlist_url}: {e}")
        return []

def fetch_video_title(video_id):
    """Fetches a video title from its ID for use in the filename."""
    try:
        yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
        return yt.title
    except Exception as e:
        logging.warning(f"Could not fetch title for {video_id}: {e}. Using video ID as fallback.")
        return video_id

def translate_with_chunking(text, dest='mr', chunk_size=4500):
    """Translates large text by splitting it into smaller chunks."""
    translated_chunks = []
    for i in range(0, len(text), chunk_size):
        chunk = text[i:i + chunk_size]
        try:
            translated = translator.translate(chunk, dest=dest)
            translated_chunks.append(translated.text)
        except Exception as e:
            logging.error(f"Translation chunk failed: {e}")
            translated_chunks.append(chunk) # Append original chunk on error
    return "\n".join(translated_chunks)

# --- CORRECTED FUNCTION ---
def get_transcript_with_fallback(video_id):
    """
    Fetches transcripts with a fallback strategy:
    1. Manually created Marathi
    2. Auto-generated Marathi
    3. Translated from any available English transcript
    """
    try:
        # Fetch the list of all available transcripts once.
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
    except (TranscriptsDisabled, NoTranscriptFound) as e:
        # If transcripts are disabled or no transcripts exist at all,
        # raise the original error to be caught by the calling function.
        raise e

    # Strategy 1: Find manually created Marathi transcript
    try:
        transcript = transcript_list.find_manually_created_transcript(['mr'])
        logging.info(f"Using original Marathi captions for {video_id}")
        return "\n".join([t['text'] for t in transcript.fetch()])
    except NoTranscriptFound:
        pass  # Not found, continue to the next strategy

    # Strategy 2: Find auto-generated Marathi transcript
    try:
        transcript = transcript_list.find_generated_transcript(['mr'])
        logging.info(f"Using auto-generated Marathi captions for {video_id}")
        return "\n".join([t['text'] for t in transcript.fetch()])
    except NoTranscriptFound:
        pass  # Not found, continue to the next strategy

    # Strategy 3: Find any English transcript and translate it
    try:
        transcript = transcript_list.find_transcript(['en', 'en-US', 'en-GB'])
        original_text = "\n".join([t['text'] for t in transcript.fetch()])
        logging.info(f"Translating English captions to Marathi for {video_id}")
        return translate_with_chunking(original_text)
    except NoTranscriptFound:
        pass  # English transcript not found either

    # If all strategies fail, raise the definitive error.
    # This is caught by the process_video function, which logs it gracefully.
    raise NoTranscriptFound(video_id, [t.language_code for t in transcript_list], {})


def process_caption_text(text, prompt):
    """Applies an optional prompt to the final caption text."""
    if not prompt:
        return text
    return f"{text}\n\n{prompt}"

def save_caption_file(filename, text):
    """Saves the final text to a file in the captions directory."""
    os.makedirs(CAPTIONS_DIR, exist_ok=True)
    path = os.path.join(CAPTIONS_DIR, filename)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(text)
    logging.info(f"Saved caption: {filename}")


def process_video(video_url, prompt):
    """Main processing logic for a single video URL."""
    video_id = None
    try:
        # Robustly extract video ID from various URL formats
        if 'youtu.be' in video_url:
            video_id = parse.urlparse(video_url).path.lstrip('/')
        else:
            qs = parse.parse_qs(parse.urlparse(video_url).query)
            if 'v' in qs:
                video_id = qs['v'][0]
        
        if not video_id or not re.match(r'^[a-zA-Z0-9_-]{11}$', video_id):
            logging.error(f"Invalid YouTube URL or ID: {video_url}")
            return
    except Exception as e:
        logging.error(f"Could not parse video ID from '{video_url}': {e}")
        return

    title = fetch_video_title(video_id)
    safe_title = sanitize_filename(title)
    filename = f"{safe_title}_{video_id}.txt"

    try:
        # This block now correctly handles all exceptions from get_transcript_with_fallback
        caption_text = get_transcript_with_fallback(video_id)
        processed_text = process_caption_text(caption_text, prompt)
        save_caption_file(filename, processed_text)
    except (NoTranscriptFound, TranscriptsDisabled) as e:
        logging.warning(f"Could not get captions for {video_id} ({title}): {e}")
    except Exception as e:
        # Catch any other unexpected errors during processing
        logging.error(f"An unexpected error occurred while processing {video_id}: {e}")
    finally:
        # A delay is crucial to avoid being rate-limited by YouTube/Google APIs
        time.sleep(DELAY_BETWEEN_VIDEOS)

def main():
    links_file = "links.txt"
    prompt_file = "prompt.txt"

    batches = read_batches_from_links_file(links_file)
    if not batches:
        logging.info("No links found in links.txt. Exiting.")
        return

    prompt = read_prompt(prompt_file)
    if prompt:
        logging.info(f"Loaded prompt from {prompt_file}")

    logging.info(f"Processing {len(batches)} batch(es)...")
    for i, batch in enumerate(batches, start=1):
        logging.info(f"--- Starting Batch {i} ---")
        
        expanded_videos = []
        for link in batch:
            if "playlist?list=" in link:
                expanded_videos.extend(fetch_video_ids_from_playlist(link))
            else:
                expanded_videos.append(link)

        logging.info(f"Found {len(expanded_videos)} total videos in batch {i}.")

        for j in range(0, len(expanded_videos), BATCH_SIZE):
            sub_batch = expanded_videos[j:j + BATCH_SIZE]
            sub_batch_num = (j // BATCH_SIZE) + 1
            
            pbar_desc = f"Batch {i}, Sub-batch {sub_batch_num}"
            for video_url in tqdm(sub_batch, desc=pbar_desc):
                process_video(video_url, prompt)
            
            if j + BATCH_SIZE < len(expanded_videos):
                logging.info(f"Finished sub-batch. Waiting {DELAY_BETWEEN_BATCHES}s...")
                time.sleep(DELAY_BETWEEN_BATCHES)

        if i < len(batches):
            logging.info(f"--- Finished Batch {i}. Waiting {DELAY_BETWEEN_BATCHES}s before next batch. ---")
            time.sleep(DELAY_BETWEEN_BATCHES)

    logging.info("All batches processed successfully.")

if __name__ == "__main__":
    main()