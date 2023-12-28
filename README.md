# Video Stream Processing with Apache Storm

## Overview

This Storm topology processes video frames using Apache Storm, a distributed real-time computation system. The topology includes spouts and bolts for reading, processing, and aggregating frames.

## Table of Contents

- [Overview](#overview)
  - [Key Features](#key-features)
  - [Components](#components) 
- [Getting Started](#getting-started)
  - [Requirements](#requirements)
  - [Installation](#installation)

## Overview
![Image Processing Toploogy](diagrams/Topology.svg)

### Key Features

- **Parallel Image Processing:** Utilize Apache Storm's parallel processing to concurrently apply image processing tasks to incoming frames.
- **Dynamic Topology:** Seamlessly integrate image processing bolts to handle various tasks, including blurring and sharpening, in a dynamic and scalable manner.
- **Aggregation:** Aggregate processed frames, combining blurred and sharpened versions, and create video outputs.

### Components

- **ImageSpout:** Reads video files, extracts frames, and performs initial image processing.
- **ImageProcessingBolt:** Applies image processing tasks, such as blurring, to frames in parallel.
- **ImageSharpeningBolt:** Applies image sharpening tasks to frames in parallel.
- **AggregationBolt:** Aggregates blurred and sharpened frames, creating a final set of processed frames.



## Getting Started

### Requirements
Please install these tooll and libraries before running the project:
- [Java](https://www.java.com/): The project requires Java.(I have used OpenJDK 11 but you can use any version that you want.)
- [Apache Storm](http://storm.apache.org/) installed and configured.(The version I have used is 2.6.0.)
- [Apache Zookeeper](https://zookeeper.apache.org/) installed and configured.(The version I have used is 3.8.3 stable release version.)
- [OpenCV](https://opencv.org/) installed.(The version I have used is 4.8.0 but any other version works fine👌.)

### Installation
1. Install the tools and libraries mentioned in the [Requirements](#requirements) section.
2. Clone the Repository
3. Go to './[video-stream-processing or any other name that you have chosen for the main project folder]/PE/src/'
4. Compile and run ImageProcessingTopology.java.
5. Have fun with It😊😊😊.

