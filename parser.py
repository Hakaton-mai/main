import re
import requests
from bs4 import BeautifulSoup
import ujson
import pika

# Set headers for the requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
}

# URL template for the comments pages
URL_TEMPLATE = "https://www.banki.ru/services/responses/bank/promsvyazbank/?page={page_index}&is_countable=on"

# RabbitMQ connection settings
RABBITMQ_HOST = 'localhost'
RABBITMQ_QUEUE = 'test'

def clean_comment_text(text: str) -> str:
    """
    Clean up the text of the comment by removing unwanted characters.
    """
    return re.sub(r'[^a-zA-Zа-яА-ЯёЁ0-9\s.,?!\-:;()]+', '', text)

def get_comments(page_index: int):
    """
    Fetch and parse comments from the specified page.
    """
    url = URL_TEMPLATE.format(page_index=page_index)

    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()  # Raise an error for bad status codes
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page_index}: {e}")
        return []

    # Parse the HTML page
    soup = BeautifulSoup(response.text, "html.parser")

    # Look for the JSON data embedded in a script tag
    script_tag = soup.find("script", {"type": "application/ld+json"})

    if script_tag:
        try:
            data = script_tag.string.strip().replace('&quot;', '').replace('&lt;p&gt;', '').replace('&lt;/p&gt;', '').replace('\u00A0', '')
            data = re.sub(r'\\[^"\\/bfnrtu]', '', data)
            data = ujson.loads(data)

            reviews = data.get('review', [])
            if not reviews:
                print(f"No reviews found on page {page_index}.")
                return []

            return [clean_comment_text(record['description']) for record in reviews]

        except (ujson.JSONDecodeError, KeyError) as e:
            print(f"Error parsing JSON on page {page_index}: {e}")
            return []
    else:
        print(f"Script tag with JSON not found on page {page_index}.")
        return []

def send_to_rabbitmq(comments):
    """
    Send comments to a RabbitMQ queue.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    for comment in comments:
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body="{\"msg\":\" "+ comment + " \"}",
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
        )
        print(f"Sent comment to RabbitMQ: {comment[:50]}...")

    connection.close()

def main():
    page_index = 31
    while True:
        comments = get_comments(page_index)
        if not comments:
            break
        print(f"Page {page_index} comments: {len(comments)} found.")
        send_to_rabbitmq(comments)
        page_index += 1

if __name__ == "__main__":
    main()
