import os
import json
from datetime import datetime
from time import sleep
import ast
import shutil
import regex as re
import g4f
from moviepy.editor import VideoFileClip
import requests
from requests.exceptions import ConnectionError, ReadTimeout
from rich import print
from rich.progress import Progress, TextColumn, TimeElapsedColumn, MofNCompleteColumn, BarColumn, TaskProgressColumn

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
TOTAL_DOWNLOAD_TRAFFIC = 0
FAILED_DOWNLOADS = 0

# config
NUM_VIDEOS_TO_DOWNLOAD = 20000  # Downloads in chronological order, recommended between 100 - 500 unless you have a lot of storage.
# end config

def download_file(url, output_file_path):
    while True:
        try:
            response = requests.get(url, stream=True, timeout=30)  # Add timeout argument
            if response.status_code == 200:
                with open(output_file_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
                        global TOTAL_DOWNLOAD_TRAFFIC
                        TOTAL_DOWNLOAD_TRAFFIC += len(chunk)
                return True
            else:
                if not str(output_file_path).endswith('.jpg'):
                    print("[red3]Failed to download file - HTTP Code " + str(response.status_code))
                return False
        except ReadTimeout:
            print("[gold1]Connection timed out. Waiting 30 seconds and retrying.[/gold1]")
            sleep(30)
        except ConnectionError:
            print("[gold1]Failed to download a file. Waiting 10 seconds and retrying.[/gold1]")
            sleep(10)

def get_video_length(file_path):
    try:
        video = VideoFileClip(file_path)
        duration = int(video.duration)
        hours = duration // 3600
        minutes = (duration % 3600) // 60
        seconds = duration % 60

        if hours > 0:
            length_formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        else:
            length_formatted = f"{minutes:02d}:{seconds:02d}"

        return length_formatted
    except Exception as e:
        print(f"Error: {e}")
        return None

def custom_split(text, delimiter):
    text = text.replace("\xa0", "")
    text = text.replace("&amp;", "&amp-placeholder")
    pattern = fr"(?<=gvibirID|gvibirDESC|gvibirLEN|gvibirDATE|gvibirPIC|gvibirURL|{re.escape(delimiter)})|(?={re.escape(delimiter)}(?!gvibir))"
    parts = [item.replace("&amp-placeholder", "&amp;").strip(";") for item in re.split(pattern, text) if item and not item.startswith("gvibir")]
    combined_parts = []
    temp = ""
    for part in parts:
        if not part:
            temp += ";"
        else:
            combined_parts.append(temp + part)
            temp = ""
    combined_lst = []
    for item in combined_parts:
        if item.startswith(";;") and combined_lst:
            combined_lst[-1] += item
        else:
            combined_lst.append(item.strip(";"))
    return combined_lst

def processVideo(raw_data):
    data = raw_data
    raw_data = custom_split(raw_data.rstrip(), ";")
    raw_data = [item.rstrip() for item in raw_data if not item.startswith("gvibir")]
    id = raw_data[0]
    if(os.path.exists(os.path.join(os.path.join(OUTPUT_DIR, raw_data[0]), os.path.basename("failed") + ".json"))):
        print("[dark_orange3]Video with ID " + raw_data[0] + " is unarchived, skipping.[/dark_orange3]")
        return True
    try:
        os.makedirs(os.path.join(OUTPUT_DIR, raw_data[0]), False)
    except FileExistsError:
        print("[gold3]Video with ID " + raw_data[0] + " already exists, skipping.")
        return True
    except OSError:
        print("[steel_blue1]Failed to programatically process input. Attempting to send through ChatGPT (L1)")
        try:
            response = g4f.ChatCompletion.create(model='gpt-3.5-turbo', provider=g4f.Provider.GetGpt, messages=[{"role": "user", "content": "Extract the metadata from the string provided. Return the text as a python list, e.g. \"['value1', 'value2', 'value3']\". Do not explain anything, just output the python list. First string of numbers is the ID. \"gvibirID\" is the video title. \"gvibirDESC\" is the video description. \"gvibirLEN\" is the length of the video. It is sometimes empty, or not stated. In the case that it is, simply do not add that value to the output python list. Do not try to substitute the length of the video with another value, just ignore it if it is empty. \"gvibirDATE\" is the date uploaded. It contains a string of numbers, and a string of text separated by a comma. These should be all one value. \"gvibirPIC\" is the thumbnail URL. \"gvibirURL\" is the video url. Remove any escape characters if found. Write the response in a code box. Do not include the variable name. Use double quotes instead of single quotes to wrap the list values, and escape any double quotes that appear in the data. Input string: " + data}])
        except Exception as e:
            print(e)
            print("[red1]Failed to process video " + raw_data[0] + ". Skipping.")
            return False
        try:
            raw_data = ast.literal_eval(response)
        except SyntaxError:
            print("[red1]Failed to process video " + raw_data[0] + ". This is likely because ChatGPT gave a wrong answer. The folder has been deleted and on the next run, it should try again. For debugging purposes, the response has been printed.")
            print(response)
            return False
        os.makedirs(os.path.join(OUTPUT_DIR, raw_data[0]), False)
    success = download_file(raw_data[-1], os.path.join(os.path.join(OUTPUT_DIR, raw_data[0]), os.path.basename("videoplayback") + ".flv"))
    if success:
        length = raw_data[3]
        # convert weird date format to epoch timestamp -- timezone appears to be DST (GMT-07:00)
        try:
            uploaded_epoch = datetime.strptime(raw_data[4].split(",")[0], "%Y%m%d%H%M%S").timestamp()
        except ValueError:
            # sometimes the meta lacks video length, so let's make a workaround and calculate it on our own
            try:
                uploaded_epoch = datetime.strptime(raw_data[3].split(",")[0], "%Y%m%d%H%M%S").timestamp()
                print("[steel_blue1]Video with ID " + raw_data[0] + "'s meta is missing video length, calculating from file.")
                length = get_video_length(os.path.join(os.path.join(OUTPUT_DIR, raw_data[0]), os.path.basename("videoplayback") + ".flv"))
            except ValueError as e:
                # if we still struggle with it, the data formatting must be wrong because i suck at regex. let's send it through ai instead 👍
                print("[steel_blue1]Failed to programmatically process input. Attempting to send through ChatGPT.[/steel_blue1]")
                try:
                    response = g4f.ChatCompletion.create(model='gpt-3.5-turbo', provider=g4f.Provider.GetGpt, messages=[{"role": "user", "content": "Extract the metadata from the string provided. Return the text as a python list, e.g. \"['value1', 'value2', 'value3']\". Do not explain anything, just output the python list. First string of numbers is the ID. \"gvibirID\" is the video title. \"gvibirDESC\" is the video description. \"gvibirLEN\" is the length of the video. It is sometimes empty, or not stated. In the case that it is, simply do not add that value to the output python list. Do not try to substitute the length of the video with another value, just ignore it if it is empty. \"gvibirDATE\" is the date uploaded. It contains a string of numbers, and a string of text separated by a comma. These should be all one value. \"gvibirPIC\" is the thumbnail URL. \"gvibirURL\" is the video url. Remove any escape characters if found. Write the response in a code box. Do not include the variable name. Use double quotes instead of single quotes to wrap the list values, and escape any double quotes that appear in the data. Input string: " + data}])
                except Exception as e:
                    print(e)
                    print("[red1]Failed to process video " + raw_data[0] + ". Skipping.")
                    shutil.rmtree(os.path.join(OUTPUT_DIR, id))
                    return False
                try:
                    raw_data = ast.literal_eval(response)
                except SyntaxError:
                    print("[red1]Failed to process video " + raw_data[0] + ". This is likely because ChatGPT gave a wrong answer. The folder has been deleted and on the next run, it should try again. For debugging purposes, the response has been printed.")
                    print(response)
                    shutil.rmtree(os.path.join(OUTPUT_DIR, id))
                    return False
                try:
                    uploaded_epoch = datetime.strptime(raw_data[4].split(",")[0], "%Y%m%d%H%M%S").timestamp()
                    length = raw_data[3]
                except ValueError:
                    try:
                        uploaded_epoch = datetime.strptime(raw_data[3].split(",")[0], "%Y%m%d%H%M%S").timestamp()
                    except:
                        print("[red1]Failed to process video " + raw_data[0] + ". Skipping.")
                        shutil.rmtree(os.path.join(OUTPUT_DIR, id))
                        return False
                    print("[steel_blue1]Video with ID " + raw_data[0] + "'s meta is missing video length, calculating from file.")
                    length = get_video_length(os.path.join(os.path.join(OUTPUT_DIR, raw_data[0]), os.path.basename("videoplayback") + ".flv"))
                except:
                    print("[red1]Failed to process video " + raw_data[0] + ". Skipping.")
                    shutil.rmtree(os.path.join(OUTPUT_DIR, id))
                    return False
                print("[green]✔[/green] [spring_green2]Got a valid response back.[/spring_green2]")
        data = {"id": raw_data[0], "title": raw_data[1], "description": raw_data[2], "length": length, "uploaded": uploaded_epoch}
        with open(os.path.join(os.path.join(OUTPUT_DIR, raw_data[0]), os.path.basename("metadata") + ".json"), 'w', encoding='utf-8') as file:
            file.write(json.dumps(data, indent=4, ensure_ascii=False))
        thumb_success = download_file(raw_data[-2], os.path.join(os.path.join(OUTPUT_DIR, raw_data[0]), os.path.basename("thumbnail") + ".jpg"))
        if not thumb_success:
            print("[red3]Thumbnail download failed for video " + raw_data[0])
    else:
        print("Failed to download video " + raw_data[0] + ". Is it archived?")
        data = {"id": raw_data[0], "error": 404}
        with open(os.path.join(os.path.join(OUTPUT_DIR, raw_data[0]), os.path.basename("failed") + ".json"), 'w', encoding='utf-8') as file:
            file.write(json.dumps(data, indent=4, ensure_ascii=False))

if __name__ == "__main__":
    with open("metaunsorted.txt", 'r', encoding='utf-8') as file:
        with Progress(TextColumn("[progress.description]{task.description}"), BarColumn(), TaskProgressColumn(), MofNCompleteColumn(), TimeElapsedColumn(), TextColumn("[sky_blue3]{task.fields[dl_traffic]}[/sky_blue3]"), auto_refresh=True) as progress:
            task = progress.add_task("[sky_blue3]Downloading videos...", total=NUM_VIDEOS_TO_DOWNLOAD, dl_traffic=str(round(TOTAL_DOWNLOAD_TRAFFIC / (1024*1024), 2)) + " MB")

            for line_number in range(NUM_VIDEOS_TO_DOWNLOAD):
                line = file.readline().strip()
                if line:
                    if not processVideo(line):
                        FAILED_DOWNLOADS += 1
                progress.update(task, advance=1, dl_traffic=str(round(TOTAL_DOWNLOAD_TRAFFIC / (1024*1024), 2)) + " MB")
    
    count = 0
    with os.scandir(OUTPUT_DIR) as entries:
        for entry in entries:
            if entry.is_dir():
                count += 1
    print("[spring_green2]Download complete.[/spring_green2] [grey39]" + str(NUM_VIDEOS_TO_DOWNLOAD - FAILED_DOWNLOADS) + " videos failed to download.")
