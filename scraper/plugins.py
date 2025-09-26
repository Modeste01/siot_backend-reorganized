from bs4 import BeautifulSoup  # For parsing and navigating HTML
import re  # Regular expressions
import functools
from datetime import date, timedelta, datetime, timezone
from datetime import time as datetimetime

# THE GAME SQL TABLE
# date Date,
# time timestamp,
# Away_Team varchar(100),
# Home_Team varchar(100),
# Score json,
# Winner varchar(100),
# Sport varchar(50),
# Primary Key(Home_Team, Away_Team, Sport),
# FOREIGN KEY(Away_Team, sport) REFERENCES School(Name, sport),
# FOREIGN KEY(Home_Team, sport) REFERENCES School(Name, sport),
# FOREIGN KEY(Winner, sport) REFERENCES School(Name, sport)

class TypeASport:
    def __init__(self, soup):
        """Parse common sport event data from HTML."""
        self.soup = soup

        # Matches date with optional time (e.g., MM/DD/YYYY or MM/DD/YYYY HH:MM AM/PM)
        date_pattern = re.compile(r"\d{2}/\d{2}/\d{4}(?:\s+\d{1,2}:\d{2}\s+(?:AM|PM))?")

        # Extract common elements
        date_time_element = self.soup.find(string=date_pattern)
        date_time = date_time_element.strip() if date_time_element else "Date not found"
        date_only = None
        if date_time == "Date not found":
            # Default to today at 00:00 UTC if not found
            date_time_dt = datetime.combine(date.today(), datetimetime(0, 0)).replace(tzinfo=timezone.utc)
        else:
            date_only = date_time.split(maxsplit=1)[0]
            if "TBA" in date_time or len(date_time.split()) == 1:
                # No time present
                date_time_dt = datetime.strptime(date_only, "%m/%d/%Y").replace(tzinfo=timezone.utc)
            else:
                date_time_dt = datetime.strptime(date_time, "%m/%d/%Y %I:%M %p").replace(tzinfo=timezone.utc)

        attendance_element = self.soup.find("div", class_="col p-0 text-right")
        attendance = attendance_element.get_text(strip=True).replace("Attend: ", "") if attendance_element else "Unknown"

        live_box_score_link = self.soup.find("a", target="LIVE_BOX_SCORE")
        period_element = self.soup.find("span", id=re.compile(r"^period_\d+"))
        clock_element = self.soup.find("span", id=re.compile(r"^clock_\d+"))
        scores_present = any(cell.get_text(strip=True) for cell in self.soup.find_all("div", id=re.compile(r"^score_\d+")))

        if live_box_score_link and not scores_present:
            # No scores yet, but we have a live link => Not started
            game_status = "Not Started"
            game_link = live_box_score_link["href"]
        elif period_element:
            # Check the text in the period element
            current_period = period_element.get_text(strip=True)

            # Final vs live
            if current_period in ["F", "Final"]:
                game_status = "Final"
                box_score_link = self.soup.find("a", target=re.compile(r"box_score_\d+"))
                game_link = box_score_link["href"] if box_score_link else "Link not found"
                current_clock = "00:00"
            else:
                game_status = "In Progress"
                game_link = live_box_score_link["href"] if live_box_score_link else "Link not found"
                current_clock = clock_element.get_text(strip=True) if clock_element else "Unknown"
        else:
            # Assume final if no period element
            game_status = "Final"
            box_score_link = self.soup.find("a", target=re.compile(r"box_score_\d+"))
            game_link = box_score_link["href"] if box_score_link else "Link not found"
            current_clock = "00:00"

        # Extract teams and scores
        teams = []
        scores = []
        team_rows = self.soup.find_all("tr", id=re.compile(r"contest_\d+"))
        for team_row in team_rows:
            team_name_element = team_row.find("td", class_="opponents_min_width")
            if team_name_element:
                team_name = team_name_element.get_text(strip=True).split(" (")[0]
                teams.append(team_name)

            score_element = team_row.find("div", class_="p-1")
            score = score_element.get_text(strip=True) if score_element and score_element.get_text(strip=True) else ""
            if score.isdigit():
                scores.append(int(score.replace(",", "")))

        # Prepare common game info dictionary
        self.game_info = {
            "date": (date_only or date_time.split(maxsplit=1)[0]) if date_time != "Date not found" else date.today().strftime("%m/%d/%Y"),
            "time": date_time_dt,
            "attendance": int(attendance.replace(",", "")) if attendance.replace(",", "").isdigit() else None,
            "status": game_status,
            "home_team": teams[1] if len(teams) > 1 else None,
            "away_team": teams[0] if len(teams) > 0 else None,
            "score": scores if scores_present else "Not yet available",
            "game_link": game_link
        }

        # Add current period/clock if game is in progress
        if game_status == "In Progress":
            self.game_info["current_period"] = current_period
            self.game_info["current_clock"] = current_clock

        if self.game_info["status"] != "Final":
            self.game_info["winner"] = None
        elif isinstance(self.game_info["score"], list) and len(self.game_info["score"]) >= 2 and self.game_info["score"][0] == self.game_info["score"][1]:
            self.game_info["winner"] = "tie"
        elif isinstance(self.game_info["score"], list) and len(self.game_info["score"]) >= 2 and self.game_info["score"][0] > self.game_info["score"][1]:
            self.game_info["winner"] = teams[0]
        else:
            self.game_info["winner"] = teams[1] if len(teams) > 1 else None


class Volleyball(TypeASport):
    def __init__(self, soup):
        super().__init__(soup)
        sport_data = {}
        # Volleyball: variable number of sets (up to 5)
        set_scores = []
        linescore_table = self.soup.find("table", id=re.compile(r"linescore_\d+_table"))
        if linescore_table:
            for score_row in linescore_table.find_all("tr"):
                set_data = [int(cell.get_text(strip=True))
                    for cell in score_row.find_all("td") if cell.get_text(strip=True).isdigit()]
                set_scores.append(set_data)
        sport_data["set_scores"] = set_scores
        self.game_info["sport_details"] = sport_data

class TypeBSport:
    def __init__(self, soup):
        pass


class MensBasketball(TypeASport):
    def __init__(self, soup):
        super().__init__(soup)
        sport_data = {}
        # Basketball: typically 4 quarters, possibly with overtime periods
        period_scores = []
        linescore_table = self.soup.find("table", id=re.compile(r"linescore_\d+_table"))
        if linescore_table:
            for score_row in linescore_table.find_all("tr"):
                period_data = [int(cell.get_text(strip=True))
                    for cell in score_row.find_all("td") if cell.get_text(strip=True).isdigit()]
                period_scores.append(period_data)
            sport_data["period_scores"] = period_scores
        self.game_info["sport_details"] = sport_data

class WomensBasketball(TypeASport):
    def __init__(self, soup):
        super().__init__(soup)
        sport_data = {}
        # Basketball: typically 4 quarters, possibly with overtime periods
        period_scores = []
        linescore_table = self.soup.find("table", id=re.compile(r"linescore_\d+_table"))
        if linescore_table:
            for score_row in linescore_table.find_all("tr"):
                period_data = [int(cell.get_text(strip=True))
                   for cell in score_row.find_all("td") if cell.get_text(strip=True).isdigit()]
                period_scores.append(period_data)
            sport_data["period_scores"] = period_scores
        self.game_info["sport_details"] = sport_data

class Baseball(TypeASport):
    def __init__(self, soup):
        super().__init__(soup)

        score = []
        team_rows = soup.select('tr[id^="contest_"]')
        for row in team_rows:
            team_name = row.select_one('td.opponents_min_width a').text.strip()
            #score = int(row.select_one('td.totalcol div').text.strip())
            hits = int(row.select_one('td.hitscol div').text.strip())
            errors = int(row.select_one('td.errorscol div').text.strip())
            score.append([hits, errors])

        self.game_info["score"] = score

          
class Football(TypeASport):
    def __init__(self, soup):
        super().__init__(soup)

class WomensSoccer(TypeASport):
    def __init__(self, soup):
        super().__init__(soup)

class MensSoccer(TypeASport):
    def __init__(self, soup):
        super().__init__(soup)
