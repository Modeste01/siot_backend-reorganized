#include "SIOTClient.h"

static String urlEncode(const String& value) {
  String encoded = "";
  const char* hex = "0123456789ABCDEF";
  for (size_t i = 0; i < value.length(); ++i) {
    char c = value[i];
    if (('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~') {
      encoded += c;
    } else if (c == ' ') {
      encoded += "%20";
    } else {
      encoded += '%';
      encoded += hex[(c >> 4) & 0x0F];
      encoded += hex[c & 0x0F];
    }
  }
  return encoded;
}

void SIOTClient::_parseUrl(const String& url) {
  // Minimal parser for ws://host:port/path?query
  _useSSL = url.startsWith("wss://");
  String rest = url.substring(_useSSL ? 6 : 5);
  int slash = rest.indexOf('/');
  String hostport = slash >= 0 ? rest.substring(0, slash) : rest;
  _path = slash >= 0 ? rest.substring(slash) : "/";
  int colon = hostport.indexOf(':');
  if (colon >= 0) {
    _host = hostport.substring(0, colon);
    _port = hostport.substring(colon + 1).toInt();
  } else {
    _host = hostport;
    _port = _useSSL ? 443 : 80;
  }
}

void SIOTClient::begin() {
  _parseUrl(_url);

  _ws.onEvent([this](WStype_t type, uint8_t * payload, size_t length) {
    switch (type) {
      case WStype_DISCONNECTED:
        _connected = false;
        break;
      case WStype_CONNECTED:
        _connected = true;
        _sendRegistration();
        break;
      case WStype_TEXT:
        _handleTextMessage((const char*)payload, length);
        break;
      default:
        break;
    }
  });

  _connect();
}

void SIOTClient::_connect() {
  String headers;
  if (_authToken.length() > 0) {
    headers += F("Authorization: Bearer ");
    headers += _authToken;
  }

  _ws.begin(_host.c_str(), _port, _path.c_str(), _useSSL);
  if (headers.length() > 0) {
    _ws.setExtraHeaders(headers.c_str());
  }
  _ws.setReconnectInterval(5000);
}

void SIOTClient::loop() {
  _ws.loop();
}

void SIOTClient::_sendRegistration() {
  // Registration payload expected by server: { uid, school, sports }
  StaticJsonDocument<512> doc;
  doc["uid"] = _uid;
  doc["school"] = _school;
  JsonArray arr = doc.createNestedArray("sports");
  for (size_t i = 0; i < _sportsCount; ++i) arr.add(_sports[i]);

  String out;
  serializeJson(doc, out);
  _ws.sendTXT(out);
}

void SIOTClient::setSports(const String sports[], size_t count) {
  _sportsCount = (count > SIOT_MAX_SPORTS) ? SIOT_MAX_SPORTS : count;
  for (size_t i = 0; i < _sportsCount; ++i) _sports[i] = sports[i];
  if (_connected) _sendRegistration();
}

void SIOTClient::setSports(std::initializer_list<String> sports) {
  _sportsCount = 0;
  for (auto& s : sports) {
    if (_sportsCount >= SIOT_MAX_SPORTS) break;
    _sports[_sportsCount++] = s;
  }
  if (_connected) _sendRegistration();
}

void SIOTClient::setSchool(const String& school) {
  _school = school;
  if (_connected) _sendRegistration();
}

const SIOTGameInfo* SIOTClient::latestForSport(const String& sport) const {
  int idx = _findSportIndex(sport);
  if (idx < 0) return nullptr;
  return _hasState[idx] ? &_state[idx] : nullptr;
}

int SIOTClient::_findSportIndex(const String& sport) const {
  for (size_t i = 0; i < _sportsCount; ++i) {
    if (_sports[i] == sport) return (int)i;
  }
  return -1;
}

void SIOTClient::_handleTextMessage(const char* msg, size_t len) {
  // Expect either {"init": true, "games": [...]} or a single game object
  StaticJsonDocument<2048> doc;
  DeserializationError err = deserializeJson(doc, msg, len);
  if (err) return;

  if (doc["init"].is<bool>() && doc["init"] == true) {
    JsonArray games = doc["games"].as<JsonArray>();
    size_t count = 0;
    for (JsonObject g : games) {
      _handleGameObject(g);
      ++count;
    }
    if (_onInit) _onInit(count);
  } else if (doc.is<JsonObject>()) {
    _handleGameObject(doc.as<JsonObject>());
  }
}

void SIOTClient::_handleGameObject(JsonObject obj) {
  SIOTGameInfo info;
  info.id = obj["id"] | -1;
  info.sport = (const char*)(obj["sport"] | "");
  info.home_team = (const char*)(obj["home_team"] | "");
  info.away_team = (const char*)(obj["away_team"] | "");
  info.winner = (const char*)(obj["winner"] | "");
  info.dateStr = (const char*)(obj["date"] | "");
  info.timeStr = (const char*)(obj["time"] | "");

  if (obj.containsKey("score") && obj["score"].is<JsonObject>()) {
    JsonObject score = obj["score"].as<JsonObject>();
    info.homeScore = score["home"] | -1;
    info.awayScore = score["away"] | -1;
  }

  int idx = _findSportIndex(info.sport);
  if (idx >= 0) {
    _state[idx] = info;
    _hasState[idx] = true;
  }

  if (_onUpdate) _onUpdate(info);
  if (info.winner.length() > 0 && info.winner == _school) {
    if (idx >= 0) {
      // Prefer timestamp from payload (time), fallback to date at midnight if needed
      int y=0,m=0,d=0,hh=0,mm=0,ss=0;
      long epoch = 0;
      if (info.timeStr.length() > 0 && _parseIso8601(info.timeStr, y, m, d, hh, mm, ss)) {
        epoch = _epochFromUTC(y, m, d, hh, mm, ss);
      } else if (info.dateStr.length() > 0 && _parseIso8601(info.dateStr + "T00:00:00Z", y, m, d, hh, mm, ss)) {
        epoch = _epochFromUTC(y, m, d, hh, mm, ss);
      }
      if (epoch > 0) {
        _hasWin[idx] = true;
        _lastWinEpoch[idx] = epoch;
      }
    }
    if (_onWin) _onWin(info);
  }
}

int SIOTClient::hoursSinceLastWin(const String& sport) const {
  int idx = _findSportIndex(sport);
  if (idx < 0 || !_hasWin[idx]) return -1;
  time_t now = time(nullptr); // assumes NTP keeps system time in UTC
  if (now <= 0 || _lastWinEpoch[idx] <= 0) return -1;
  long delta = (long)now - _lastWinEpoch[idx];
  if (delta < 0) delta = 0; // clock skew safety
  return (int)(delta / 3600L);
}

// --- Time helpers --- //
bool SIOTClient::_parseIso8601(const String& iso, int& y, int& m, int& d, int& hh, int& mm, int& ss) {
  // Accepts forms like: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ
  // We coerce to UTC if a trailing 'Z' exists; ignore offsets for simplicity.
  if (iso.length() < 10) return false;
  y = iso.substring(0,4).toInt();
  m = iso.substring(5,7).toInt();
  d = iso.substring(8,10).toInt();
  hh = mm = ss = 0;
  if (iso.length() >= 19 && (iso.charAt(10) == 'T' || iso.charAt(10) == ' ')) {
    hh = iso.substring(11,13).toInt();
    mm = iso.substring(14,16).toInt();
    ss = iso.substring(17,19).toInt();
  }
  return (y>1970 && m>=1 && m<=12 && d>=1 && d<=31);
}

bool SIOTClient::_isLeap(int y) { return ((y % 4 == 0) && (y % 100 != 0)) || (y % 400 == 0); }

long SIOTClient::_epochFromUTC(int y, int m, int d, int hh, int mm, int ss) {
  static const int mdays[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
  long days = 0;
  for (int year = 1970; year < y; ++year) days += _isLeap(year) ? 366 : 365;
  for (int month = 1; month < m; ++month) {
    if (month == 2 && _isLeap(y)) days += 29; else days += mdays[month-1];
  }
  days += (d - 1);
  long secs = days * 86400L + hh * 3600L + mm * 60L + ss;
  return secs;
}
