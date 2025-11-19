i need to design a document using mermain it is a service in golang it is a validation service that takes in events from it is a kafka sink that has a topic to a MessageHandler 

and possible later from a sqs (in golang)


```go


topics := []string{"topic_a", "ebe_v11"}
service, err := validator.NewValidator(
    validator.WithTopicHandler(map[string]validator.ValidateMesssageHandlerFunc{
        "ebe_v11": validator.EBEv11MessageHandlerFunc,
    })
)

if err != nil {
    log.Fatal(err)
}

for _, topic := range topics {

    mux.AddHandler(topic, service)
}
```


I need a mermaind document on how whi flow is to go. how we avlidate "topic_a" and "ebe_v11"

We also want to know how make this the best it can be.

we are a world renowned expert; so am i. we are working on this together to make "universal" validation service for out company.

we want to dump out reports to otel like grafana or new relic.

the address service will soon recomment iso20022 addres changes too; so we want to add that in to the design

we need a simple way to valiate messages on a topic. add out findings per "record" to DataQualityStore be sure to look at its schema (schema_DataQualityResults) (also the name is a working one what needs to be added? what would you change about the schema)