(ns kixi.ifore-devices)

;; Not Uploaded
;; Wattbox Build ID,Date Time,K,Relay Command,Tank Heated Volume,Prestart Time,BooOccupantPresent,Temp Profile Selected,Temp SetPoint,More Heat Button Push,More Heat Profile Adjust,Less Heat Button Push,Less Heat Profile Adjust,Hot Water Button Push,Hot Water Activated, ... HW Tank Body Temperature,HW Tank Base Temperature,Hot Tap Feed Temperature ... electricityConsumption

;; Date example: "25/11/2011 00:58:58"

(def device-definition-map
  {:ControllingRoom_Temp
   {:description "ControllingRoom_Temp"
    :privacy "private"
    :location {:name "thermostat room"}
    :metadata {:customer_ref "Living Room or Hallway Temp"}
    :readings [{:type       "temperatureAmbient"
                :unit       "C"
                :accuracy   0
                :resolution 60
                :period     "INSTANT"
                :max        99
                :min        -10}]}

   :External_Temp
   {:description "External_Temp"
    :privacy "private"
    :location    {:name "external"}
    :metadata    {:customer_ref "External Temperature"}
    :readings    [{:type       "temperatureAmbient"
                   :unit       "C"
                   :accuracy   0
                   :resolution 60
                   :period     "INSTANT"
                   :max        99
                   :min        -50}]}

   :External_Humidity
   {:description "External_Humidity"
    :privacy "private"
    :location    {:name "External"}
    :metadata    {:customer_ref "External Humidity"}
    :readings    [{:type       "relativeHumidity"
                   :unit       "%RH"
                   :accuracy   0
                   :resolution 60
                   :period     "INSTANT"
                   :max        100
                   :min        0}]}

   :LoungeA_Temp
   {:description "LoungeA_Temp"
    :privacy "private"
    :location    {:name "lounge"}
    :metadata    {:customer_ref "Lounge Temperature 1"}
    :readings    [{:type       "temperatureAmbient"
                   :unit       "C"
                   :accuracy   0
                   :resolution 60
                   :period     "INSTANT"
                   :max        30
                   :min        0}]}

   :LoungeA_Humidity
   {:description "LoungeA_Humidity"
    :privacy "private"
    :location    {:name "Lounge"}
    :metadata    {:customer_ref "Lounge Humidity"}
    :readings    [{:type       "relativeHumidity"
                   :unit       "%RH"
                   :accuracy   0
                   :resolution 60 
                   :period     "INSTANT"
                   :max        100
                   :min        0}]}

   :LoungeB_Temp
   {:description "LoungeB_Temp"
    :privacy "private"
    :location    {:name "lounge B"}
    :metadata    {:customer_ref "Lounge Temperature 2"}
    :readings    [{:type       "temperatureAmbient"
                   :unit       "C"
                   :accuracy   0
                   :resolution 60
                   :period     "INSTANT"
                   :max        30
                   :min        0}]}

   :LoungeB_CO2
   {:description "LoungeB_CO2"
    :privacy "private"
    :location    {:name "Lounge B"}
    :metadata    {:customer_ref "Lounge CO2"}
    :readings    [{:type       "CO2"
                   :unit       "ppm"
                   :accuracy   0
                   :resolution 60
                   :period     "INSTANT"
                   :max        10000
                   :min        0}]}

   :Bedroom1_Temp
   {:description "Bedroom1_Temp"
    :privacy "private"
    :location    {:name "Bedroom"}
    :metadata    {:customer_ref "Bedroom1 Temperature"}
    :readings    [{:type       "temperatureAmbient"
                   :unit       "C"
                   :accuracy   0
                   :resolution 60
                   :period     "INSTANT"
                   :max        30
                   :min        0}]}

   :Bedroom1_Humidity
   {:description "Bedroom1_Humidity"
    :privacy "private"
    :location    {:name "Bedroom1"}
    :metadata    {:customer_ref "Bedroom1 Humidity"}
    :readings    [{:type       "relativeHumidity"
                   :unit       "%RH"
                   :accuracy   0
                   :resolution 60
                   :period     "INSTANT"
                   :max        100
                   :min        0}]}

   :KitchenA_Temp
   {:description "KitchenA_Temp"
    :privacy "private"
    :location    {:name "KitchenA"}
    :metadata    {:customer_ref "Kitchen Temperature"}
    :readings    [{:type       "temperatureAmbient"
                   :unit       "C"
                   :accuracy   0
                   :resolution 60
                   :period     "INSTANT"
                   :max        30
                   :min        0}]}

   :KitchenA_Humidity
   {:description "KitchenA_Humidity"
    :privacy "private"
    :location    {:name "KitchenA"}
    :metadata    {:customer_ref "Kitchen Humidity"}
    :readings    [{:type       "relativeHumidity"
                   :unit       "%RH"
                   :accuracy   0
                   :resolution 60
                   :period     "INSTANT"
                   :max        100
                   :min        0}]}


   :ElecLoadforWattBox
   {:description "ElecLoadforWattBox"
    :privacy "private"
    :metadata    {:customer_ref "Electric Meter Current"}
    :readings    [{:type       "electricityConsumption"
                   :unit       "W"
                   :accuracy   0
                   :resolution 60
                   :period     "PULSE"
                   :max        9999
                   :min        0}]}

   :GasMeterPulse
   {:description "GasMeterPulse"
    :privacy "private"
    :metadata    {:customer_ref "Gas Meter Pulse"}
    :readings    [{:type       "gasConsumption"
                   :unit       "m^3"
                   :accuracy   0
                   :resolution 60
                   :period     "PULSE"
                   :max        999999
                   :min        0}]}})
