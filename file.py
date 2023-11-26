import requests
import csv
from retrying import retry
import time
import concurrent.futures
import threading

 
token = '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'
headers = {
    'Authorization': f'Bearer {token}',
    'Accept': 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
}

# Set a timeout value for the requests
request_timeout = 10  # 10 seconds

# Define a retry strategy
@retry(wait_fixed=10000, stop_max_attempt_number=3)
def send_request(url, headers):
    return requests.get(url, headers=headers, timeout=request_timeout)

# Function to fetch repository data and write to CSV
def fetch_and_write(item, writer, counter_lock, continue_flag):
    global counter
    # Check the continue flag before processing
    if not continue_flag[0]:
        return

    repo_id = item['id']
    repo_name = item['name']
    owner_login = item['owner']['login']
    followers_url = item['owner']['followers_url']
    repo_html_url = item['html_url']
    repo_description = item['description']
    languages_url = item['languages_url']
    stargazers_url = item['stargazers_url']

    # Additional API requests to get specific data
    followers_response = send_request(followers_url, headers)
    followers_count = len(followers_response.json())

    languages_response = send_request(languages_url, headers)
    languages_data = languages_response.json()
    languages = ', '.join(list(languages_data.keys()))

    stargazers_response = send_request(stargazers_url, headers)
    stargazers_count = len(stargazers_response.json())

    # Use a lock to update the counter atomically
    with counter_lock:
        current_counter = counter
        counter += 1

        # Check if the counter exceeded 1000000
        if current_counter > 1000000:
            continue_flag[0] = False
            print("Counter exceeded 1000000. Stopping the loop.")
            return

    # Write the data to the CSV file
    writer.writerow({
        'Counter': current_counter,
        'Repository ID': repo_id,
        'Repository Name': repo_name,
        'Owner': owner_login,
        'Followers Count': followers_count,
        'Repository HTML URL': repo_html_url,
        'Description': f'"{repo_description}"', 
        'Languages': languages,
        'Stars Count': stargazers_count
    })
    print(current_counter)

# Initial API endpoint
url = 'https://api.github.com/repositories'

# Initialize a counter and a lock
counter = 1
counter_lock = threading.Lock()

# Flag to indicate whether the loop should continue
continue_loop = [True]

# Create a CSV file for writing
with open('github_repositories.csv', 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['Counter', 'Repository ID', 'Repository Name', 'Owner', 'Followers Count', 'Repository HTML URL', 'Description', 'Languages', 'Stars Count']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    # Write the header row to the CSV file
    writer.writeheader()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Use executor to parallelize requests
        futures = []
        while continue_loop[0] and counter <= 1000000:
            try:
                response = send_request(url, headers)

                if response.status_code == 200:
                    data = response.json()

                    for item in data:
                        # Check the continue flag and counter before submitting a new task
                        with counter_lock:
                            if not continue_loop[0] or counter > 1000000:
                                break

                        # Create a future for each repository
                        future = executor.submit(fetch_and_write, item, writer, counter_lock, continue_loop)
                        futures.append(future)

                    # Check if there's a 'next' link in the response headers
                    link_header = response.headers.get('Link')
                    if link_header:
                        next_url = next_link = None
                        links = link_header.split(', ')
                        for link in links:
                            parts = link.split('; ')
                            if len(parts) == 2 and parts[1] == 'rel="next"':
                                next_url = parts[0].strip('<>')
                                break

                        if next_url:
                            url = next_url
                        else:
                            url = None
                    else:
                        url = None

                elif response.status_code == 403:
                    # Handle rate limiting by waiting and then retrying with exponential backoff
                    rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', 0))
                    wait_time = max(rate_limit_reset - int(time.time()) + 1, 1)
                    print(f"Waiting for rate limit reset ({wait_time} seconds)...")
                    time.sleep(wait_time)
                    continue

                else:
                    print(f"Failed to retrieve data. Status code: {response.status_code}")
                    url = None
                    time.sleep(3)  # Wait a bit before retrying

            except requests.exceptions.Timeout:
                print("Request timed out. Retrying...")
                time.sleep(3)  # Wait before retrying

        # Wait for all futures to complete
        concurrent.futures.wait(futures)

    # Shut down the executor to ensure all threads are completed
    executor.shutdown()

print("Data saved to 'github_repositories.csv'")
 