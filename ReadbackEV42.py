import Send_Kafka_Event
import datetime
from streaming_data_types.eventdata_ev42 import deserialise_ev42 # import ESS Flatbuffer serialiser
import matplotlib.pyplot as plt
import pandas as pd

mins = 45
print_terminal = True
plot = True
df_spectrum_map = pd.read_csv('MAPS_SPEC_MAP.csv')
df_spectrum_map.Detector_id = df_spectrum_map.Detector_id.astype(int)
df_spectrum_map.Spectrum = df_spectrum_map.Spectrum.astype(int)

start_time = (datetime.datetime.now() - datetime.timedelta(minutes=mins))
end_time = datetime.datetime.now()
print("Getting Data: ")
KafkaEvents = Send_Kafka_Event.get_data_between(start_time, end_time)

total_events=0
if print_terminal:
    if KafkaEvents is not None:
        for i in KafkaEvents:
            RawEvent = deserialise_ev42(i)
            print(f"pulse time: {RawEvent.pulse_time} detID: {RawEvent.detector_id} TOF: {RawEvent.time_of_flight}")
            total_events += len(RawEvent.detector_id)
    print("Kafka Event Len:",len(KafkaEvents))
    print("total events: ", total_events)
    print("Events/Min: ", total_events/mins)
    print("Events/Sec: ", total_events/mins/60)


def event_to_spectrum(input_event):
    if input_event in spectrum_map_detectorid:
        spec_map_i = spectrum_map_detectorid.index(input_event)
        event_spec = spectrum_map_spectrum[spec_map_i]
    else:
        event_spec = 0
        print(input_event)
    return event_spec

if plot:
    print("plot starting...")
    df_spectrum_histo = pd.DataFrame(df_spectrum_map.Spectrum.unique(), columns=['spectrum'])
    df_spectrum_histo["count"] = 0
    print(df_spectrum_histo.head())
    print(df_spectrum_map.head())
    print("spec map read complete")
    # spectrum_map_detectorid = df_spectrum_map['Detector_id'].tolist()
    # spectrum_map_spectrum = df_spectrum_map['Spectrum'].tolist()
    # spectrum_histo_spectrum = df_spectrum_histo['spectrum'].tolist()
    # spectrum_histo_count = df_spectrum_histo['count'].tolist()

    spectrum_map_detectorid = []
    spectrum_map_spectrum = []
    spectrum_histo_spectrum = [0]   # has index 0 for spec 0
    spectrum_histo_count = [0]

    spectrum_map_detectorid.extend(df_spectrum_map['Detector_id'].tolist())
    spectrum_map_spectrum.extend(df_spectrum_map['Spectrum'].tolist())
    spectrum_histo_spectrum.extend(df_spectrum_histo['spectrum'].tolist())
    spectrum_histo_count.extend(df_spectrum_histo['count'].tolist())

    print("Tables setup")
    print(f"Num packets: {len(KafkaEvents)}")

    total_events = 0
    spec_0 = 0
    if KafkaEvents is not None:
        id = 0
        id_total = len(KafkaEvents)

        # convert kafka list to event list
        print("Unpacking kafka data")
        RawEvents = [deserialise_ev42(i) for i in KafkaEvents]
        print("Data unpacked from flatbuffer")

        # create list with raw detector IDs
        print("Creating master dect id list...")
        RawDetectors = []
        for RawEvent in RawEvents:
            if len(RawEvent.detector_id) != 0:
                for event in RawEvent.detector_id:
                    RawDetectors.append(event)

        print("list complete, now mapping to spectra")
        # convert event list to spectrum list
        spectra_events = [event_to_spectrum(det_id) for det_id in RawDetectors]

        print("complete, now histogramming")
        # histo the list
        for event in spectra_events:
            total_events += 1
            spectrum_histo_index = spectrum_histo_spectrum.index(event_to_spectrum(event))
            spectrum_histo_count[spectrum_histo_index] += 1

        # for RawEvent in RawEvents:
        #     if len(RawEvent.detector_id) != 0:
        #         for event in RawEvent.detector_id:
        #             total_events += 1
        #             spectrum_histo_index = spectrum_histo_spectrum.index(event_to_spectrum(event))
        #             spectrum_histo_count[spectrum_histo_index] += 1
        #     print(f"{id}/{id_total} - num event:{total_events}, spec0:{spec_0}")
        #     id += 1

        plt.plot(spectrum_histo_spectrum, spectrum_histo_count)
        plt.show()


    print(total_events)
