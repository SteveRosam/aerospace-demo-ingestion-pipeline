# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
from quixstreams import Application, State
import time
import os

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

# add timestamp
def add_timestamp(row: dict, state: State):
    time_0 = state.get("time_0", default=None)
    if not time_0:
        state.set("time_0", int(time.time()*1000))
        time_0 = state.get("time_0", default=None)
    row["new_timestamp"] = time_0 + row["timestamp"]


def main():

    # Setup necessary objects
    app = Application(
        consumer_group="data-norm-v1-dev3",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    input_topic = app.topic(name=os.environ["input"])
    output_topic = app.topic(name=os.environ["output"])
    sdf = app.dataframe(topic=input_topic)

    # Do StreamingDataFrame operations/transformations here
    sdf = sdf.print(metadata=True)
    sdf = sdf.apply(lambda row: row["data"], expand=True)
    

    # Add timestamp
    sdf = sdf.apply(lambda row: add_timestamp(row), stateful=True)
    sdf = sdf.set_timestamp(lambda value, key, timestamp, headers: value['new_timestamp'])

    sdf = sdf.print(metadata=True)

    # Finish off by writing to the final result to the output topic
    #sdf.to_topic(output_topic)

    # With our pipeline defined, now run the Application
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()