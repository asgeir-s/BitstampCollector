BitstampCollector
=================

Lifesycle
-----------------

STEP1 - Download cvs-zip from Bitcoincharts (the newest trade can be many hours old) and writes it to the database. To the Tick-table.

STEP2 - Generate granularity tables from the tick-table.
Granularity Tables:
  - min1
  - min2
  - min5
  - min10
  - min15
  - min30
  - hour1
  - hour2
  - hour5
  - hour12
  - day

STEP3 - Retreve newest trades from Bitcoincharts (can be 15 min delayed). Used because Bitstamp api might not have that old trades.

STEP4 - Retreve newest trades from Bitstamp. To breach the gap between the 15 min delayed trades received from Bitcoincharts and new live trades.

STEP5 - Listen for live trades from Bitstamp websocket.
