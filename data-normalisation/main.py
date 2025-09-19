# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
from quixstreams import Application

import os

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

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
    sdf = sdf.print(metadata=True)

    # Finish off by writing to the final result to the output topic
    #sdf.to_topic(output_topic)

    # With our pipeline defined, now run the Application
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()