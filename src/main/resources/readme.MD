行程表
  driverId UInt64,
  isStart String,
  endTime DateTime,
  startTime DateTime,
  startLon Float32,
  startLat Float32,
  endLon Float32,
  endLat Float32,
  passengerCnt UInt16,
  rideId UInt64,
  taxiId UInt64
  
费用表 
  driverId UInt64,
  rideId UInt64,
  taxiId UInt64,
  startTime DateTime,
  paymentType String,
  tip Float32,
  tolls Float32,
  totalFare Float32