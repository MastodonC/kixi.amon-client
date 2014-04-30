(ns kixi.units)

(def amon-units
  [{:description "absoluteHumidity" :unit "g/Kg" :type "Number"}
   {:description "barometricPressure" :unit "mbar" :type "Number"}
   {:description "co2" :unit "ppm" :type "Number"}
   {:description "currentSignal" :unit "mA" :type "Number"}
   {:description "electricityAmps" :unit "Amps" :type "Number"}
   {:description "electricityConsumption" :unit "kWh" :type "Number"}
   {:description "electricityExport" :unit "kWh" :type "Number"}
   {:description "electricityFrequency" :unit "Hz" :type "Number"}
   {:description "electricityGeneration" :unit "kWh" :type "Number"}
   {:description "electricityImport" :unit "kWh" :type "Number"}
   {:description "electricityKiloVoltAmpHours" :unit "kVArh" :type "Number"}
   {:description "electricityKiloWatts" :unit "kW" :type "Number"}
   {:description "electricityVolts" :unit "V" :type "Number"}
   {:description "electricityVoltAmps" :unit "VA" :type "Number"}
   {:description "electricityVoltAmpsReactive" :unit "VAr" :type "Number"}
   {:description "flowRateAir" :unit "m^3/h" :type "Number"}
   {:description "flowRateLiquid" :unit "Ls^-1" :type "Number"}
   {:description "gasConsumption" :unit "m^3, ft^3,kWh" :type "Number"}
   {:description "heatConsumption" :unit "kWh" :type "Number"}
   {:description "heatExport" :unit "kWh" :type "Number"}
   {:description "heatGeneration" :unit "kWh" :type "Number"}
   {:description "heatImport" :unit "kWh" :type "Number"}
   {:description "heatTransferCoefficient" :unit "W/m^2.K" :type "Number"}
   {:description "liquidFlowRate" :unit "Litres/5min" :type "Number"}
   {:description "oilConsumption" :unit "m^3, ft^3,kWh" :type "Number"}
   {:description "powerFactor" :unit "" :type "Number (0-1)"}
   {:description "pulseCount" :unit "" :type "Number"}
   {:description "relativeHumidity" :unit "%RH" :type "Number"}
   {:description "relativeHumidity" :unit "wm-2" :type "Number"}
   {:description "solarRadiation" :unit "W/m^2" :type "Number"}
   {:description "status" :unit "" :type "Number (0/1)"}
   {:description "temperatureAir" :unit "C" :type "Number"}
   {:description "temperatureAmbient" :unit "C" :type "Number"}
   {:description "temperatureFluid" :unit "C" :type "Number"}
   {:description "temperatureGround" :unit "C" :type "Number"}
   {:description "temperatureRadiant" :unit "C" :type "Number"}
   {:description "temperatureSurface" :unit "C" :type "Number"}
   {:description "thermalEnergy" :unit "kWhth" :type "Number"}
   {:description "time" :unit "millisecs" :type "Number"}
   {:description "voltageSignal" :unit "mV" :type "Number"}
   {:description "waterConsumption" :unit "L" :type "Number"}
   {:description "windDirection" :unit "degrees" :type "Number"}
   {:description "windSpeed" :unit "ms^-1" :type "Number"}])
