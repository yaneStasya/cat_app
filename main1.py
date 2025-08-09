import requests
import logging
import json
import time
from tqdm import tqdm
from config import Config  

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cat_uploader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class YaDiskUploader:

    def __init__(self, token):
        self.token = token
        self.headers = {'Authorization': f'OAuth {token}'}
        self.base_url = 'https://cloud-api.yandex.net/v1/disk/resources'

    def create_folder(self, folder_name):
        url = f'{self.base_url}?path={folder_name}'
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            if response.status_code == 404:
                logger.info(f"Folder '{folder_name}' not found. Creating...")
                response = requests.put(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                logger.info(f"Folder '{folder_name}' created successfully.")
            elif response.status_code == 200:
                logger.info(f"Folder '{folder_name}' already exists.")
            else:
                response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Error creating folder '{folder_name}': {e}")
            raise

    def get_upload_link(self, path):
        url = f'{self.base_url}/upload?path={path}&overwrite=true'
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            return response.json()['href']
        except requests.RequestException as e:
            logger.error(f"Error getting upload link for '{path}': {e}")
            raise
        except KeyError:
            logger.error(f"Unexpected response format when getting upload link for '{path}'")
            raise ValueError("Invalid API response format")

    def upload_from_url(self, yandex_path, file_url, max_retries=3):
        for attempt in range(max_retries + 1):
            try:
                upload_link = self.get_upload_link(yandex_path)
                response = requests.post(upload_link, data={'url': file_url}, timeout=60)
                response.raise_for_status()
                
                time.sleep(2)
                return response.json() if response.content else {}
            except requests.RequestException as e:
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(f"Upload attempt {attempt + 1} failed for '{yandex_path}': {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All upload attempts failed for '{yandex_path}': {e}")
                    raise

    def get_file_size(self, path):
        url = f'{self.base_url}?path={path}'
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            info = response.json()
            return info.get('size', 0)
        except requests.RequestException as e:
            logger.error(f"Error getting file size for '{path}': {e}")
            return 0


class CatImageFetcher:

    def __init__(self):
        self.base_url = 'https://cataas.com/cat/says/{text}?json=true'

    def get_cat_image_url(self, text, max_retries=3):
        if not text:
            raise ValueError("Text cannot be empty")

        url = self.base_url.format(text=text)
        for attempt in range(max_retries + 1):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()
                if 'url' in data:
                    return f"https://cataas.com{data['url']}"
                else:
                    return f"https://cataas.com/cat/says/{text}"
            except requests.RequestException as e:
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(f"Fetch attempt {attempt + 1} failed for text '{text}': {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All fetch attempts failed for text '{text}': {e}")
                    raise
            except (ValueError, KeyError) as e:
                logger.error(f"Invalid response format for text '{text}': {e}")
                return f"https://cataas.com/cat/says/{text}"


def validate_config(config):
    if not config.YANDEX_DISK_TOKEN:
        raise ValueError("YANDEX_DISK_TOKEN is missing in config")
    if not config.YANDEX_DISK_FOLDER:
        raise ValueError("YANDEX_DISK_FOLDER is missing in config")
    if not config.CAT_TEXTS or not isinstance(config.CAT_TEXTS, list):
        raise ValueError("CAT_TEXTS should be a non-empty list")

    if not config.YANDEX_DISK_FOLDER.replace('-', '').replace('_', '').isalnum():
        logger.warning(f"Folder name '{config.YANDEX_DISK_FOLDER}' contains special characters that might cause issues.")


def main():
    try:
        config = Config()
        validate_config(config)

        logger.info("Starting cat image upload process...")
        logger.info(f"Target folder: {config.YANDEX_DISK_FOLDER}")
        logger.info(f"Number of images to process: {len(config.CAT_TEXTS)}")

        uploader = YaDiskUploader(config.YANDEX_DISK_TOKEN)
        fetcher = CatImageFetcher()

        uploader.create_folder(config.YANDEX_DISK_FOLDER)

        files_info = []
        successful = 0
        failed = 0

        for text in tqdm(config.CAT_TEXTS, desc="Uploading cats"):
            try:
                logger.info(f"Fetching cat image with text: '{text}'")
                cat_url = fetcher.get_cat_image_url(text)
                filename = f"{text.replace(' ', '_')}.jpg"
                yandex_path = f'/{config.YANDEX_DISK_FOLDER}/{filename}'
                logger.info(f"Uploading '{filename}' to Yandex Disk at '{yandex_path}'")
                uploader.upload_from_url(yandex_path, cat_url)

                size = uploader.get_file_size(yandex_path)
                if size > 0:
                    logger.info(f"Uploaded '{filename}' successfully, size: {size} bytes")
                    status = 'success'
                    successful += 1
                else:
                    logger.warning(f"Uploaded '{filename}' but file size is zero or unknown")
                    status = 'uploaded_but_size_unknown'
                    successful += 1

                files_info.append({
                    'filename': filename,
                    'text': text,
                    'size': size,
                    'path': yandex_path,
                    'status': status
                })

            except Exception as e:
                logger.error(f"Failed to process '{text}': {e}")
                failed += 1
                files_info.append({
                    'filename': f"{text.replace(' ', '_')}.jpg",
                    'text': text,
                    'size': 0,
                    'path': f'/{config.YANDEX_DISK_FOLDER}/{text.replace(" ", "_")}.jpg',
                    'status': 'failed',
                    'error': str(e)
                })

        json_filename = 'files_info.json'
        try:
            with open(json_filename, 'w', encoding='utf-8') as fjson:
                json.dump({
                    'summary': {
                        'total': len(config.CAT_TEXTS),
                        'successful': successful,
                        'failed': failed,
                        'folder': config.YANDEX_DISK_FOLDER
                    },
                    'files': files_info
                }, fjson, ensure_ascii=False, indent=4)
            logger.info(f"Saved upload information to '{json_filename}'")
        except Exception as e:
            logger.error(f"Error saving JSON file '{json_filename}': {e}")

        logger.info(f"Process finished: {successful} successful, {failed} failed.")

        if failed > 0:
            logger.warning(f"{failed} uploads failed, please check logs.")
            return 1

        return 0

    except Exception as e:
        logger.critical(f"Critical error occurred: {e}")
        return 1


if __name__ == '__main__':
    exit(main())