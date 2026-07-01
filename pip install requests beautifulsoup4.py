
import requests
from bs4 import BeautifulSoup

def decode_secret_message(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    rows = soup.find_all('tr')
    data = []
    for row in rows[1:]:  # skip the header row
        cols = row.find_all('td')
        if len(cols) == 3:
            try:
                x = int(cols[0].get_text(strip=True))
                char = cols[1].get_text(strip=True)
                y = int(cols[2].get_text(strip=True))
                data.append((x, y, char))
            except:
                continue
    
    max_x = max(d[0] for d in data)
    max_y = max(d[1] for d in data)
    
    grid = [[' '] * (max_x + 1) for _ in range(max_y + 1)]
    
    for x, y, char in data:
        grid[y][x] = char
    
    for row in grid:
        print(''.join(row))