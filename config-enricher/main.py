import os

from quixstreams import Application
from quixstreams.dataframe.joins.lookups.quix_configuration_service import QuixConfigurationService
from quixstreams.dataframe.joins.lookups.quix_configuration_service.lookup import JSONField


def get_fields():
    return {
        "throttle": JSONField(**{
            "type": "TestConfig",
            "jsonpath": "throttle.value"
        }),
        "hold_time": JSONField(**{
            "type": "TestConfig",
            "jsonpath": "hold_time.value"
        }),
        "battery-id": JSONField(**{
            "type": "TestConfig",
            "jsonpath": "battery.id"
        }),
        "motor-id": JSONField(**{
            "type": "TestConfig",
            "jsonpath": "motor.id"
        }),
        "shroud-id": JSONField(**{
            "type": "TestConfig",
            "jsonpath": "shroud.id"
        }),
        "fan-id": JSONField(**{
            "type": "TestConfig",
            "jsonpath": "fan.id"
        })
    }



def main():
    app = Application(
        consumer_group=os.environ["CONSUMER_GROUP"],
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    data_topic = app.topic(name=os.environ["DATA_TOPIC"], key_deserializer="str")
    config_topic = app.topic(name=os.environ["CONFIG_TOPIC"])
    output_topic = app.topic(name=os.environ["OUTPUT_TOPIC"], key_serializer="str")

    app.dataframe(topic=data_topic).join_lookup(
        lookup=QuixConfigurationService(
            topic=config_topic,
            app_config=app.config,
        ),
        fields=get_fields()
    ).to_topic(output_topic)

    app.run()


if __name__ == "__main__":
    main()