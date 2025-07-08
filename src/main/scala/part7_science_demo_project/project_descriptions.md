# The science project

---

A full stack application with
- A web server with Akka HTTP
- Kafka
- Spark Structured Streaming

---

## A scientific study

- The effects of alcohol/substances/Scala on reflexes and response times
- The mechanism
  - the test subject sees a red square on a browser window, must click it as fast as possible
  - when the test subject clicks on the square it will reset randomly on the screen
  - repeat 100 times
- The data gathering
  - on every click, the deltaTime is sent to the web server
  - the web server sends the deltaTime to a streaming pipeline
  - deltaTimes are aggregated on a rolling average of the past 10 values
- Tech Stack
  - a simple HTML/JS interface
  - a web server with Akka HTTP with a REST endpoint
  - Kafka
  - Spark Structured Streaming