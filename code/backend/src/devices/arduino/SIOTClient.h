#pragma once

#include <Arduino.h>
#include <WebSocketsClient.h>
#include <ArduinoJson.h>
#include <time.h>

#ifndef SIOT_MAX_SPORTS
#define SIOT_MAX_SPORTS 8
#endif

struct SIOTGameInfo {
  int id = -1;
  String sport;
  String home_team;
  String away_team;
  String winner;   // May be empty until final
  int homeScore = -1;
  int awayScore = -1;
  String dateStr;  // ISO date string
  String timeStr;  // ISO timestamp string or empty
};

class SIOTClient {
public:
  typedef std::function<void(size_t)> InitCallback;                   // Called after initial snapshot; provides number of games
  typedef std::function<void(const SIOTGameInfo&)> UpdateCallback;    // Called on each update received
  typedef std::function<void(const SIOTGameInfo&)> WinCallback;       // Called when winner == school

  // url example: ws://192.168.1.10:8000/ws/12345  (the path should include /ws/{uid})
  SIOTClient(const String& url, const String& uid, const String& school, const String& authToken = "")
      : _url(url), _uid(uid), _school(school), _authToken(authToken) {}

  void begin();
  void loop();
  bool isConnected() const { return _connected; }

  // Set sports list (replaces previous), sends registration payload when connected
  void setSports(const String sports[], size_t count);
  void setSports(std::initializer_list<String> sports);
  void setSchool(const String& school);

  // Callbacks
  void onInit(InitCallback cb) { _onInit = std::move(cb); }
  void onUpdate(UpdateCallback cb) { _onUpdate = std::move(cb); }
  void onWin(WinCallback cb) { _onWin = std::move(cb); }

  // Access latest known state for a sport; returns nullptr if unknown
  const SIOTGameInfo* latestForSport(const String& sport) const;

  // Returns hours since the last win for the configured school in this sport.
  // Returns -1 if no win has been seen yet for this sport.
  int hoursSinceLastWin(const String& sport) const;

private:
  WebSocketsClient _ws;
  bool _connected = false;

  String _url;
  String _host;
  uint16_t _port = 80;
  String _path;
  bool _useSSL = false;

  String _uid;
  String _school;
  String _authToken;

  String _sports[SIOT_MAX_SPORTS];
  size_t _sportsCount = 0;

  // Per-sport latest state
  SIOTGameInfo _state[SIOT_MAX_SPORTS];
  bool _hasState[SIOT_MAX_SPORTS] = {false};

  // Per-sport last win tracking (epoch seconds, UTC)
  long _lastWinEpoch[SIOT_MAX_SPORTS] = {0};
  bool _hasWin[SIOT_MAX_SPORTS] = {false};

  // Time helpers
  static bool _parseIso8601(const String& iso, int& y, int& m, int& d, int& hh, int& mm, int& ss);
  static bool _isLeap(int y);
  static long _epochFromUTC(int y, int m, int d, int hh, int mm, int ss);

  InitCallback _onInit;
  UpdateCallback _onUpdate;
  WinCallback _onWin;

  void _parseUrl(const String& url);
  void _connect();
  void _sendRegistration();
  void _handleTextMessage(const char* msg, size_t len);
  void _handleGameObject(JsonObject obj);
  int _findSportIndex(const String& sport) const;
};
