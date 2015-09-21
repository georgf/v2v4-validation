/*
 * This is to be used from a Firefox scratchpad:
 * - enable chrome devtools: in about:config set "devtools.chrome.enabled" to true
 * - open scratchpad: Tools -> Web Developer -> Scratchpad
 * - make it run as chrome: choose Environment -> Browser
 * - click "Run"
 *
 * After scanning the local archives this should open a new tab which highlights
 * potential issues in color:
 * - v2/v4 comparisons
 * - v4 subsession consistency
 */

(function() {

Cu.import("resource://gre/modules/TelemetryArchive.jsm");
Cu.import("resource://gre/modules/TelemetryController.jsm");
Cu.import("resource://gre/modules/TelemetryUtils.jsm");

const BUILDID_CUTOFF = 20150722000000;
const MS_IN_A_DAY = 24 * 60 * 60 * 1000;
const SHOW_EXTENDED = false;

function getMainWindow() {
  return window.QueryInterface(Ci.nsIInterfaceRequestor)
               .getInterface(Ci.nsIWebNavigation)
               .QueryInterface(Ci.nsIDocShellTreeItem)
               .rootTreeItem
               .QueryInterface(Ci.nsIInterfaceRequestor)
               .getInterface(Ci.nsIDOMWindow);
}

function showTextInNewTab(str) {
  let win = getMainWindow();
  let tab = win.gBrowser.addTab("data:text/plain," + encodeURIComponent(str));
  win.gBrowser.selectedTab = tab;
}

function showHtmlInNewTab(str) {
  let win = getMainWindow();
  let tab = win.gBrowser.addTab("data:text/html," + encodeURIComponent(str));
  win.gBrowser.selectedTab = tab;
}

function mapToObject(m) {
  let o = {};
  for (let [k,v] of m) {
    o[k] = m.get(k);
  }
  return o;
}

function lastElement(array) {
  return array[array.length - 1];
}

function v2DefaultBrowserValueToV4(defaultValue) {
  switch (defaultValue) {
    case 1: return true;
    case 0: return false;
    default: return null;
  }
}

function* extractDailyV2Measurement(dailyMap, measurement, name) {
  let data = yield measurement.getValues();
  for (let [day,value] of [...Iterator(data.days)]) {
    day = day.toISOString();
    if (!dailyMap.has(day)) {
      dailyMap.set(day, {});
    }
    dailyMap.get(day)[name] = mapToObject(value);
  }
}

function* getRawV2Data() {
  const reporter = Cc["@mozilla.org/datareporting/service;1"].getService().wrappedJSObject.healthReporter;
  yield reporter.onInit();

  const payload = yield reporter.collectAndObtainJSONPayload(true);
  return payload;
}

function getV2Extract(rawV2Data) {
  const now = new Date();
  const payload = rawV2Data;
  const dailyMap = new Map();

  for (let day of Object.keys(payload.data.days)) {
    const data = payload.data.days[day];
    let extract = {
      isDefaultBrowser: null,
      searchCounts: {},
      totalTime: 0,
      cleanTotalTime: 0,
      cleanTotalTimes: [],
      abortedTotalTime: 0,
      abortedTotalTimes: [],
    };
    let dayHasData = false;

    const counts = data["org.mozilla.searches.counts"];
    if (counts) {
      for (let k of Object.keys(counts)) {
        if (k != "_v") {
          extract.searchCounts[k] = counts[k];
          dayHasData = true;
        }
      }
    }

    const appinfo = data["org.mozilla.appInfo.appinfo"];
    if (appinfo) {
      extract.isDefaultBrowser = appinfo.isDefaultBrowser;
      dayHasData = true;
    }

    const previous = data["org.mozilla.appSessions.previous"];
    if (previous) {
      const sum = (array) => array.reduce((a,b) => a+b, 0);
      extract.totalTime = sum(previous.cleanTotalTime || previous.abortedTotalTime);
      extract.abortedTotalTimes = (previous.abortedTotalTime || []);
      extract.abortedTotalTime = sum(extract.abortedTotalTimes);
      extract.cleanTotalTimes = (previous.cleanTotalTime || []);
      extract.cleanTotalTime = sum(extract.cleanTotalTimes);
      dayHasData = true;
    }

    if (dayHasData) {
      dailyMap.set(day + "T00:00:00Z", extract);
    }
  }

  const twoDig = (n) => ((n > 9) ? "" : "0") + n;
  const dateStr = `${now.getUTCFullYear()}-${twoDig(now.getUTCMonth()+1)}-${twoDig(now.getUTCDate())}T00:00:00Z`;
  if (!dailyMap.has(dateStr)) {
    dailyMap.set(dateStr, {
      searchCounts: {},
      totalTime: 0,
      cleanTotalTime: 0,
      cleanTotalTimes: [],
      abortedTotalTime: 0,
      abortedTotalTimes: [],
    });
  }
  const extract = dailyMap.get(dateStr);

  const current = payload.data.last["org.mozilla.appSessions.current"];
  extract.totalTime += current.totalTime;
  extract.cleanTotalTime += current.totalTime;
  extract.cleanTotalTimes.push(current.totalTime);

  return dailyMap;
}

function accumulateV2(dailyMap, cutoffTime) {
  let r = {
    searchCounts: {},
    totalTime: 0,
    cleanTotalTime: 0,
    abortedTotalTime: 0,
  };

  for (let [day,v] of dailyMap) {
    if ((new Date(day)).getTime() < cutoffTime) {
      continue;
    }
    for (let k of Object.keys(v.searchCounts)) {
      r.searchCounts[k] = (r.searchCounts[k] || 0) + v.searchCounts[k];
    }
    r.totalTime += v.totalTime;
    r.cleanTotalTime += v.cleanTotalTime;
    r.abortedTotalTime += v.abortedTotalTime;
  }

  return r;
}

function extractV4DataFromPing(p, isFromOldBuild = false) {
  // Build up reduced and flat ping data to work on.
  const info = p.payload.info;
  const simpleMeasurements = p.payload.simpleMeasurements;
  let data = {
    pingId: p.id,
    clientId: p.clientId,
    reason: info.reason,
    creationDate: p.creationDate,
    channel: p.application.channel,
    buildId: p.application.buildId,
    version: p.application.version,
    sessionId: info.sessionId,
    subsessionId: info.subsessionId,
    previousSessionId: info.previousSessionId,
    previousSubsessionId: info.previousSubsessionId,
    profileSubsessionCounter: info.profileSubsessionCounter,
    subsessionCounter: info.subsessionCounter,
    sessionLength: info.sessionLength,
    subsessionLength: info.subsessionLength,
    isFromOldBuild: isFromOldBuild,
    totalTime: simpleMeasurements.totalTime,
    searchCounts: {},
    isDefaultBrowser: p.environment.settings.isDefaultBrowser,
  };

  // Extract search counts.
  const h = p.payload.keyedHistograms["SEARCH_COUNTS"];
  for (let k of Object.keys(h)) {
    data.searchCounts[k] = h[k].sum;
  }

  return data;
}

function* getV4Extract() {
  // Retrieve a list of the archived main pings.
  let pings = yield TelemetryArchive.promiseArchivedPingList();
  pings = pings.filter(p => p.type == "main");

  // Load and extract data from the archived pings.
  let data = [];
  let foundNewerBuild = false;

  for (let archived of pings) {
    let p;
    try {
      p = yield TelemetryArchive.promiseArchivedPingById(archived.id);
    } catch (e) {
      // data.push({id: archived.id, timestampCreated: archived.timestampCreated, fileNotFound: true, isBroken: true});
      continue;
    }

    // Skip all leading pings from build ids that are too old.
    const isFromOldBuild = (parseInt(p.application.buildId, 10) < BUILDID_CUTOFF);
    if (!foundNewerBuild && isFromOldBuild) {
      continue;
    }
    foundNewerBuild = true;

    data.push(extractV4DataFromPing(p, isFromOldBuild));
  }

  // Push the current data on the list, otherwise we are missing
  // the measurements from the last subsession on.
  let current = TelemetryController.getCurrentPingData(true);
  data.push(extractV4DataFromPing(current));

  let previous = null;
  for (let current of data) {
    const finalReasons = new Set(["shutdown", "aborted-session", "gather-subsession-payload"]);
    current.isFinalFragment = finalReasons.has(current.reason);

    // Check for consistency issues etc.
    if (previous) {
      const c = current;
      const p = previous;
      p.isLastFragment = (p.sessionId != c.sessionId);

      c.channelSwitching = (c.channel != p.channel);
      c.brokenSessionChain = p.isFinalFragment && (c.previousSessionId != p.sessionId);
      c.brokenSubsessionChain = (c.previousSubsessionId != p.subsessionId);
      c.brokenProfileSubsessionCounter = (c.profileSubsessionCounter != (p.profileSubsessionCounter + 1));
      c.brokenSubsessionCounter = (p.isFinalFragment ?
                                          (c.subsessionCounter != 1) :
                                          (c.subsessionCounter != (p.subsessionCounter + 1)));
      c.isBroken = !c.isFromOldBuild && !p.isFromOldBuild &&
                   !c.channelSwitching &&
                   (c.brokenSessionChain ||
                    c.brokenSubsessionChain ||
                    c.brokenProfileSubsessionCounter ||
                    c.brokenSubsessionCounter);
    }

    previous = current;
  }

  data[data.length-1].isLastFragment = true;

  return data;
}

function accumulateV4(extracts, cutoffTime) {
  let r = {
    searchCounts: {},
    totalTime: 0,
    cleanTotalTime: 0,
    abortedTotalTime: 0,
    subsessionLength: 0,
    sessionLength: 0,
  };

  for (let v of extracts) {
    if ((new Date(v.creationDate)).getTime() < cutoffTime) {
      continue;
    }

    for (let k of Object.keys(v.searchCounts)) {
      r.searchCounts[k] = (r.searchCounts[k] || 0) + v.searchCounts[k];
    }

    r.subsessionLength += v.subsessionLength;
    if (v.isLastFragment) {
      r.totalTime += v.totalTime;

      if (v.reason == "aborted-session") {
        r.abortedTotalTime += v.totalTime;
      } else {
        r.cleanTotalTime += v.totalTime;
      }

      r.sessionLength += v.sessionLength || 0;
    }
  }

  return r;
}

function validateV2V4BrowserDefault(v2, v4, cutoffTime) {
  let sawBreakage = false;

  for (let [day,v2Data] of v2) {
    v2Data.brokenDefaultBrowser = false;

    const v2Time = (new Date(day)).getTime();
    if (v2Time < cutoffTime) {
      continue;
    }

    // Skip this entry if v2 didn't have any useful data.
    if (v2Data.isDefaultBrowser == null) {
      continue;
    }

    // Check for matching default entries in v4 from the same day +/- one day.
    // v2 & v4 don't match exactly, so we want to need to look
    // for matches a bit more loosely.
    // Also, v2 records this daily, v4 with every ping and hence potentially multiple
    // times a day. The best criteria here thus is "for each v2 entry, check that v4
    // also saw that value in a certain timeframe".

    v2Data.brokenDefaultBrowser = !v4.some((ping) => {
      const v4Time = (new Date(ping.creationDate)).getTime();
      if ((v4Time < (v2Time - MS_IN_A_DAY)) || (v4Time > (v2Time + MS_IN_A_DAY))) {
        return false;
      }

      return (ping.isDefaultBrowser === v2DefaultBrowserValueToV4(v2Data.isDefaultBrowser));
    });

    sawBreakage = sawBreakage || v2Data.brokenDefaultBrowser;
  }

  return sawBreakage;
}

function getV2V4Matchup(v2Extract, v4Extract, cutoffTime) {
  // Lets preprocess the v4 data into a session-oriented format first.
  let v4Data = new Map();
  const sessions = v4Extract.filter(p => p.isLastFragment);
  for (let p of sessions) {
    let startTimeUtc = Date.now();
    if (p.reason != "gather-subsession-payload") {
      // We only insert the "current" session data on todays date, everything else
      // gets attributed to the session start date.
      startTimeUtc = (new Date(p.creationDate)).getTime() - (p.totalTime * 1000);

    }
    const startDate = new Date(startTimeUtc);
    const twoDig = (n) => ((n > 9) ? "" : "0") + n;
    const startDayUtc = `${startDate.getUTCFullYear()}-` +
                        `${twoDig(startDate.getUTCMonth() + 1)}-` +
                        `${twoDig(startDate.getUTCDate())}` +
                        `T00:00:00Z`;
    if (!v4Data.has(startDayUtc)) {
      v4Data.set(startDayUtc, []);
    }

    let entry = {
      startTime: startTimeUtc,
      sessionId: p.sessionId,
      totalTime: p.totalTime,
      aborted: p.reason == "aborted-session",
      searchCounts: p.searchCounts,
      sessionLength: p.sessionLength,
      subsessionLength: v4Extract.reduce((prev, curr) => prev + ((curr.sessionId == p.sessionId) ? curr.subsessionLength : 0), 0),
    };
    // This fixes undefined sessionLength entries when the profile channel-switched to a build that
    // didn't have sessionLength yet (pre bug 1188416).
    if (entry.sessionLength === undefined) {
      entry.sessionLength = entry.subsessionLength;
    }

    v4Data.get(startDayUtc).push(entry);
  }

  // Filter out the v2 data points that have session starts.
  const v2Days = [for (v of v2Extract) if (v[1].totalTime > 0) v[0]];

  // Get the set of days we have either v2 or v4 sessions for.
  const days = [...(new Set([...v4Data.keys(), ...v2Days])).keys()];
  days.sort();
  days.reverse();

  // Build per-day session & match data.
  const data = new Map(); // day -> details
  const matchedSessionIds = new Set();
  let missingInV2Count = 0;
  let missingInV4Count = 0;

  for (let day of days) {
    if ((cutoffTime > 0) && (new Date(day).getTime() < cutoffTime)) {
      continue;
    }

    let v2 = v2Extract.get(day);
    let v4 = v4Data.get(day) || [];
    let sessions = [];

    // Add v2 sessions and try to match them with the v4 data.

    if (v2) {
      let cleanTimes = [for (t of v2.cleanTotalTimes) {time: t, aborted: false}];
      let abortedTimes = [for (t of v2.abortedTotalTimes) {time: t, aborted: true}];
      for (let t of [...cleanTimes, ...abortedTimes]) {
        // We match up sessions via time with a delta D we'd expect for
        // totalTime matches for different collection times etc.
        const D = 5;
        let match = v4.find((p) => (p.aborted == t.aborted) &&
                                   (p.aborted ||
                                     (p.totalTime >= (t.time - D)) &&
                                     (p.totalTime <= (t.time + D))) &&
                                   !matchedSessionIds.has(p.sessionId));
        if (match) {
          matchedSessionIds.add(match.sessionId);
        } else {
          ++missingInV4Count;
        }

        sessions.push({
          startTime: match ? match.startTime : 0,
          totalTimeV2: t.time,
          totalTimeV4: match ? match.totalTime : null,
          aborted: t.aborted,
          broken: !match,
          sessionId: match ? match.sessionId : null,
          sessionLength: match ? match.sessionLength : null,
          subsessionLength: match ? match.subsessionLength : null,
        });
      }
    }

    // Add unmatched v4 sessions.

    const unmatchedV4Sessions = v4.filter(p => !matchedSessionIds.has(p.sessionId));
    missingInV2Count += unmatchedV4Sessions.length;
    for (let p of unmatchedV4Sessions) {
      sessions.push({
        startTime: p.startTime,
        totalTimeV2: null,
        totalTimeV4: p.totalTime,
        aborted: p.aborted,
        broken: true,
        sessionId: p.sessionId,
        sessionLength: p.sessionLength,
        subsessionLength: p.subsessionLength,
      });
    }

    // Add the session matchups to the daily map.

    data.set(day, sessions);
  }

  const values = [].concat(...[...data.values()]);
  return {
    missingInV2Count: missingInV2Count,
    missingInV4Count: missingInV4Count,
    sessions: data,
    totalTimes: [
      values.reduce((p, c) => p + (c.totalTimeV2 || 0), 0),
      values.reduce((p, c) => p + (c.totalTimeV4 || 0), 0),
    ],
    matchedTotalTimes: [
      values.reduce((p, c) => p + (!c.broken ? (c.totalTimeV2 || 0) : 0), 0),
      values.reduce((p, c) => p + (!c.broken ? (c.totalTimeV4 || 0) : 0), 0),
    ],
    cleanTotalTimes: [
      values.reduce((p, c) => p + (!c.aborted ? (c.totalTimeV2 || 0) : 0), 0),
      values.reduce((p, c) => p + (!c.aborted ? (c.totalTimeV4 || 0) : 0), 0),
    ],
    matchedCleanTotalTimes: [
      values.reduce((p, c) => p + ((!c.aborted && !c.broken) ? (c.totalTimeV2 || 0) : 0), 0),
      values.reduce((p, c) => p + ((!c.aborted && !c.broken) ? (c.totalTimeV4 || 0) : 0), 0),
    ],
    abortedTotalTimes: [
      values.reduce((p, c) => p + (c.aborted ? (c.totalTimeV2 || 0) : 0), 0),
      values.reduce((p, c) => p + (c.aborted ? (c.totalTimeV4 || 0) : 0), 0),
    ],
    matchedAbortedTotalTimes: [
      values.reduce((p, c) => p + ((c.aborted && !c.broken) ? (c.totalTimeV2 || 0) : 0), 0),
      values.reduce((p, c) => p + ((c.aborted && !c.broken) ? (c.totalTimeV4 || 0) : 0), 0),
    ],
    sessionLength: [
      0,
      values.reduce((p, c) => p + (c.sessionLength || 0), 0),
    ],
    matchedCleanSessionLength: [
      0,
      values.reduce((p, c) => p + ((!c.aborted && !c.broken) ? (c.sessionLength || 0) : 0), 0),
    ],
    subsessionLength: [
      0,
      values.reduce((p, c) => p + (c.subsessionLength || 0), 0),
    ],
    matchedCleanSubsessionLength: [
      0,
      values.reduce((p, c) => p + ((!c.aborted && !c.broken) ? (c.subsessionLength || 0) : 0), 0),
    ],
  };
}

function renderV4Extract(extract) {
  // Fields to print in the order we want them listed.
  const printFields = [
    "creationDate",
    "pingId", "clientId", "reason", "channel", "buildId",
    "isDefaultBrowser",
    "sessionLength", "subsessionLength", "totalTime",
    "sessionId", "previousSessionId", "subsessionId", "previousSubsessionId",
    "profileSubsessionCounter", "subsessionCounter",
    "fileNotFound", "channelSwitching",
    "brokenSessionChain", "brokenSubsessionChain", "brokenProfileSubsessionCounter", "brokenSubsessionCounter",
  ];

  // Print an html table from the data.
  let text = "";
  text += "<table>";
  text += "<tr>" + [for (f of printFields) `<th>${f}</th>`].join("") + "</tr>";
  for (let d of extract) {
    text += `<tr ${d.isBroken ? ' class="broken"' : ''}>`;
    text += [for (f of printFields) `<td title="${f}">${d[f] != undefined ? d[f] : "-"}</td>`].join("");
    text += "</tr>";
  }
  text += "</table>";

  return text;
}

function renderV2Extract(data) {
  let text = "";

  text += "<table>";
  text += `<tr><th>day</th><th>default</th><th>totalTime</th><th>cleanTotalTimes</th><th>abortedTotalTimes</th><th>brokenDefaultBrowser</th></tr>`;
  for (let [day, value] of data) {
    const broken = value.brokenDefaultBrowser;
    text += `<tr ${broken ? ' class="broken"' : ''}>` +
            `<td>${day}</td><td>${value.isDefaultBrowser}</td><td>${value.totalTime}</td>` +
            `<td>${value.cleanTotalTime} = ${JSON.stringify(value.cleanTotalTimes)}</td>` +
            `<td>${value.abortedTotalTime} = ${JSON.stringify(value.abortedTotalTimes)}</td>` +
            `<td>${value.brokenDefaultBrowser}</td>` +
            `</tr>`;
  }
  text += "</table>";

  return text;
}

function renderV2V4Comparison(countsMap, v2, v4, cutoffTime, defaults) {
  const delta = (a,b) => Math.abs(a - b);
  const noInfinity = (number) => (number == Infinity) ? "-" : number;
  let text = "";

  // Some basic information.
  text += "<h3>general information</h3>";
  text += "<table>";
  text += `<tr><td>cutoff date for inspections</td>` +
          `<td>${new Date(cutoffTime)} ${(cutoffTime == 0) ? "<i>(all v2 & v4 data is recent)</i>" : ""}</td></tr>`;

  const currentBroken = (defaults.current.v2 != defaults.current.v4);
  text += `<tr><td ${currentBroken ? ' class="broken"' : ''}>current browser defaults</td>` +
          `<td>v2: ${defaults.current.v2}, v4: ${defaults.current.v4}</td></tr>`;

  text += `<tr><td ${defaults.historicallyBroken ? ' class="broken"' : ''}>` +
          `History of browser defaults broken</td><td>${defaults.historicallyBroken}` +
          `</td></tr>`;

  text += "</table>";

  // Build comparison table.
  text += "<h3>search counts</h3>";
  text += "<div>Search counts should usually line up 1:1 on fresh profiles. " +
          "However, on older profiles we can't exactly match when FHR & v4 measurements happened, " +
          "so minor discrepancies are expected there (up to a day of measurements).</div>";
  text += "<table>";

  text += "<tr><th>what</th><th>v2</th><th>v4</th><th>v4 in % of v2</tr>";
  for (let [k, v] of countsMap) {
    const prop = v[1] / v[0];
    const broken = (delta(prop, 1.0) > 0.01);
    text += `<tr ${broken ? ' class="broken"' : ''}><td>${k}</td>`;
    text += `<td>${v[0]}</td><td>${v[1]}</td>`;
    text += `<td>${noInfinity(Math.round(prop * 1000) / 10)}%`;
    text += "</tr>";
  }

  text += "</table>";
  return text;
}

function renderV2V4Matchup(matchup) {
  const delta = (a,b) => Math.abs(a - b);
  const noInfinity = (number) => (number == Infinity) ? "-" : number;

  let text = "<h3>session time matchups</h3>";

  text += "<div>The following table shows comparisons between times measured in FHR and Telemetry (all values in seconds):" +
          "<ul>" +
          "<li>matchedCleanTotalTimes - The sessions without crashes we can match from both systems. They should be very close to each other</li>" +
          "<li>matchedAbortedTotalTimes - The crashed sessions we can match from both systems. The way FHR stores session times for detection seems to make it undercount them heavily, so Telemetry is expected to have a much higher value.</li>" +
          "<li>matchedTotalTimes - The total time for sessions matched from both systems. This is expected to mismatch if there were any crashes, due to the aborted session time discrepancies.</li>" +
          "<li>totalTimes - The total time for sessions in either system. This can mismatch due to the above caveats as well as sessions that are missing from either system.</li>" +
          "<li>cleanTotalTimes - Same as above, except only for sessions without crashes.</li>" +
          "<li>abortedTotalTimes - Same as above, except only for sessions with crashes.</li>" +
          "</ul>" +
          "</div>";

  text += "<table>" +
          `<tr><th>what</th><th>v2</th><th>v4</th><th>v4 in % of v2</th></tr>`;
  let fields = [
    ["matchedCleanTotalTimes", "matchedCleanTotalTimes", 0.01],
    ["matchedAbortedTotalTimes", "matchedAbortedTotalTimes", 5.0],
    ["matchedTotalTimes", "matchedTotalTimes", 1.0],
    ["totalTimes", "totalTimes", 1.0],
    ["cleanTotalTimes", "cleanTotalTimes", 1.0],
    ["abortedTotalTimes", "abortedTotalTimes", 5.0],
  ];

  if (SHOW_EXTENDED) {
    fields = fields.concat([
      ["matchedTotalTimes", "matchedCleanSessionLength", 5.0],
      ["matchedTotalTimes", "matchedCleanSubsessionLength", 5.0],
      ["totalTimes", "sessionLength", 5.0],
      ["totalTimes", "subsessionLength", 5.0],
    ]);
  }

  for (let [fieldV2, fieldV4, tolerance] of fields) {
    const valV2 = matchup[fieldV2][0];
    const valV4 = matchup[fieldV4][1];
    const prop = valV4 / valV2;
    const broken = (delta(prop, 1.0) > tolerance);
    text += `<tr ${broken ? ' class="broken"' : ''}>` +
            `<td>${fieldV2}${(fieldV2 != fieldV4) ? ' vs. ' + fieldV4 : ''}</td>` +
            `<td>${valV2}</td>` +
            `<td>${valV4}</td>` +
            `<td>${noInfinity(Math.round(prop * 1000) / 10)}%` +
            `</tr>`;
  }
  text += "</table>";

  text += "<h3>individual session matchup</h3>";

  text += "<div>Depending on bugs in either system, we might have sessions missing in Telemetry that are recorded " +
          "in FHR or vice versa.</div>";
  text += "<table>" +
          `<tr><td>v2 sessions not matched in v4</td><td>${matchup.missingInV4Count}</td></tr>` +
          `<tr><td>v4 sessions not matched in v2</td><td>${matchup.missingInV2Count}</td></tr>` +
          "</table>";

  text += "<div>The following table tries to match up individual sessions recorded in FHR " +
          "and those in Telemetry with each other.<br>" +
          "Entries that are missing in either are highlighted.</div>";

  text += "<table>";
  text += "<tr><th>day/field</th><th>v2 totalTime</th><th>v4 totalTime</th><th>aborted</th><th>v4 session id</th>";
  if (SHOW_EXTENDED) {
    text += "<th>sessionLength</th><th>subsessionLength</th>";
  }
  text += "</tr>";

  for (let [day, values] of matchup.sessions) {
    text += `<tr class='grey'><td>${day}</td><td></td><td></td><td></td><td></td>` +
            (SHOW_EXTENDED ? "<td></td><td></td>" : "") + "</tr>";
    for (let v of values) {
      text += `<tr ${v.broken ? 'class="broken"' : ''}>` +
              `<td>${v.startTime > 0 ? new Date(v.startTime) : 'no v4 start time'}</td>` +
              `<td>${v.totalTimeV2 !== null ? v.totalTimeV2 : '-'}</td>` +
              `<td>${v.totalTimeV4 !== null ? v.totalTimeV4 : '-'}</td>` +
              `<td>${v.aborted}</td>` +
              `<td>${v.sessionId || '-'}</td>` +
              (SHOW_EXTENDED ? `<td>${v.sessionLength}</td><td>${v.subsessionLength}</td>` : "") +
              `</tr>`;
    }
  }

  text += "</table>";

  return text;
}

Task.spawn(function*() {
try {
  let v4Extract = yield* getV4Extract();
  if (v4Extract.length == 0) {
    alert("No v4 data to compare yet.");
    return;
  }

  const rawV2Data = yield* getRawV2Data();
  let v2Extract = getV2Extract(rawV2Data);
  if (v2Extract.size == 0) {
    alert("No v2 data to compare yet.");
    return;
  }

  // Build a v2/v4 comparison for the best matching historic data we can find.
  // If all v2 & v4 data is relatively recent, we use all available local data from both.
  // If the v4 data has older pings or v2s or v4s history starts more than 1 day apart,
  // we slice the data off at a roughly matching point.
  const haveOldV4Pings = v4Extract.some((p) => (parseInt(p.buildId, 10) < BUILDID_CUTOFF) ||
                                               (parseInt(p.version, 10) < 42));
  const oldestV2 = TelemetryUtils.truncateToDays(new Date(lastElement([...v2Extract.keys()]))).getTime();
  const oldestV4 = TelemetryUtils.truncateToDays(new Date(v4Extract[0].creationDate)).getTime();
  let cutoffTime = Math.max(oldestV2, oldestV4);
  if (Math.abs(oldestV2 - oldestV4) <= (MS_IN_A_DAY)) {
    cutoffTime = 0;
  }

  const v2Accumulated = accumulateV2(v2Extract, cutoffTime);
  const v4Accumulated = accumulateV4(v4Extract, cutoffTime);
  const searchKeys = new Set([...Object.keys(v2Accumulated.searchCounts), ...Object.keys(v4Accumulated.searchCounts)]);
  const counts = new Map([for (k of searchKeys) [
    "search: " + k ,
    [(v2Accumulated.searchCounts[k] || 0), (v4Accumulated.searchCounts[k] || 0)]
  ]]);

  // Extract the current default browser.
  const lastV4 = lastElement(v4Extract);
  const lastV2 = v2Extract.values().next().value;
  let defaults = {
    current: {
      v4: lastV4.isDefaultBrowser,
      v2: v2DefaultBrowserValueToV4(lastV2.isDefaultBrowser),
    },
    historicallyBroken: validateV2V4BrowserDefault(v2Extract, v4Extract, cutoffTime),
  };

  // v2/v4 session matchup.
  const v2v4Matchup = getV2V4Matchup(v2Extract, v4Extract, cutoffTime);

  // Styling.
  let text = "<style type='text/css'>" +
             "table { border-collapse: collapse; margin-bottom: 10px; }" +
             "th, td { border: solid 1px; }" +
             "tr.broken { background-color: yellow; }" +
             "div { margin-bottom: 10px; }" +
             ".grey { background-color: #E3E3E3; }" +
             "</style>";

  // Show the v2 & v4 data and data comparisons.
  text += "<h2 name='v2v4matchup'>v2/v4 matchup</h2>";
  text += "<div><i>Note:</i> v2 refers to the current FHR system, v4 to the improved Telemetry system.</div>"
  text += renderV2V4Comparison(counts, v2Accumulated, v4Accumulated, cutoffTime, defaults);
  text += renderV2V4Matchup(v2v4Matchup);

  text += "<h2>additional details</h2>";
  text += "<div>This contains more detailed data dumps that can help us understand what is going on in individual histories.</div>";

  // Enable this for some additional data dumping.
  if (true) {
    text += "<h3>v4 extract</h3>";
    v4Extract.reverse();
    text += renderV4Extract(v4Extract);
    text += "<h3>v2 extract</h3>";
    text += renderV2Extract(v2Extract);

    v2v4Matchup.sessions = mapToObject(v2v4Matchup.sessions);

    text += "<h3>dumps</h3>" + "<pre>" +
      "Accumulated v2 data:\n" +
      JSON.stringify(v2Accumulated, null, 2) +
      "\n\n" +
      "Accumulated v4 data:\n" +
      JSON.stringify(v4Accumulated, null, 2) +
      "\n\n" +
      "Accumulation comparison for cutoffTime " + new Date(cutoffTime) + "\n" +
      "Oldest v2 date: " + new Date(oldestV2) + "\n" +
      "Oldest v4 date: " + new Date(oldestV4) + "\n" +
      JSON.stringify(mapToObject(counts), null, 2) +
      "\n\n" +
      "Historic v2 data:\n" +
      JSON.stringify(mapToObject(v2Extract), null, 2) +
      "\n\n" +
      "v2/v4 matchup data:\n" +
      JSON.stringify(v2v4Matchup, null, 2) +
      "</pre>";

    text += "<h3>Raw v2 data</h3>" +
            "<pre>" + JSON.stringify(rawV2Data, null, 2) + "</pre>";

    text += "<h3>Raw v4 data</h3>" +
            "<pre>" + JSON.stringify(v4Extract, null, 2) + "</pre>";
  }

  showHtmlInNewTab(text);
} catch (e) {
  alert(e + "\n" + e.stack);
}
});

})();
