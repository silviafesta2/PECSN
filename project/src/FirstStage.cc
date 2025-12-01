/*
 * Hub.cpp
 *
 *  Created on: Nov 5, 2025
 *      Author: simo
 */

#include "FirstStage.h"
#include "RequestMessage_m.h"
#include "ClientRequestMessage_m.h"
#include <queue>

namespace project {

Define_Module(FirstStage);


// Called once at the beginning of the simulation
void FirstStage::initialize() {

    // Extracting Parameters from NED file
    numThreads = par("numThreads").intValue();
    meanServiceTime = par("meanServiceTime").doubleValue();

    // Registering Signal
    queueSize = registerSignal("queueSize");
    partialRequestTime = registerSignal("partialRequestTime");
    emit(queueSize, 0);

    // At the beginning every thread is free
    availableThreads = numThreads;
    for (int i = 0; i < numThreads; i++) {
        availableThreadIDs.push(i + 1);
    }

}

// Computes a random service delay using a uniform distribution
simtime_t FirstStage::getServiceDelay(int threadId) const {

    // Debug Logging
    EV_DEBUG << "FirstStage::getServiceDelay called. threadId: " << threadId << endl;

    return uniform(0, 2*meanServiceTime, threadId);
}

// Schedules the completion of the request
void FirstStage::scheduleRequest(long requestId, int threadId) {

    // Debug Logging
    EV_DEBUG << "FirstStage::scheduleRequest called. requestId: " << requestId
             << ", threadId: " << threadId;

    // Creating and Sending Message
    auto* endMsg = new RequestMessage("secondStage");
    endMsg->setRequestId(requestId);
    endMsg->setThreadId(threadId);
    simtime_t delay = getServiceDelay(threadId);
    scheduleAt(simTime() + delay, endMsg);

    // Logging
    EV_INFO << "Request " << requestId << " is being served. Delay: " << delay << endl;

}

// Sends a RequestMessage with a given name and request ID through a specified gate
void FirstStage::sendRequestMessage(const char* name, long requestId, int threadId, const char* gateName) {

    // Debug Logging
    EV_DEBUG << "FirstStage::sendRequestMessage called. name: " << name << ", requestId: " << requestId
             << ", threadId: " << threadId << ", gateName: " << gateName << endl;

    // Creating and Sending Request Message
    auto* msg = new RequestMessage(name);
    msg->setRequestId(requestId);
    msg->setThreadId(threadId);
    send(msg, gateName);

    // Log the sending
    EV_INFO << "Sent message '" << name << "' to gate '" << gateName;

}

// Handles a new incoming request from a client
void FirstStage::handleServe(RequestMessage* msg) {

    // Debug Logging
    EV_DEBUG << "FirstStage:handleServe called." << endl;

    // Extract Request ID from Message and Logging
    long requestId = msg->getRequestId();
    EV_INFO << "Request " << requestId << " arrived." << endl;

    // Stores arrival time of the request in an unordered map (which maps
    // request ID to arrival times) that will be used to compute partial
    // request time
    requestId2arrivalTime[requestId] = simTime();

    // If no thread is available then push into the waiting queue and log
    if (availableThreads == 0) {
        waitingRequests.push(requestId);
        emit(queueSize, waitingRequests.size());
        EV_INFO << "Request " << requestId << " queued due to no available threads." << endl;
    }

    // Otherwise start the execution and schedule the end of a request
    else {
        availableThreads--;
        int threadId = availableThreadIDs.front();
        availableThreadIDs.pop();
        scheduleRequest(requestId, threadId);
    }

}

// Handles a request that has completed the first stage
void FirstStage::handleSecondStage(RequestMessage* msg) {

    // Debug Logging
    EV_DEBUG << "FirstStage::handleSecondStage called" << endl;

    // Extract Request ID and Logging
    long requestId = msg->getRequestId();
    int threadId = msg->getThreadId();
    EV_INFO << "Request " << requestId << ", Thread ID: " << threadId << " completed first stage, forwarding to second stage." << endl;

    // Compute Partial Request Time by extracting arrival time from an hash map
    // write the statistics and remove entry from hash map
    simtime_t parReqTime = simTime() - requestId2arrivalTime[requestId];
    requestId2arrivalTime.erase(requestId);
    emit(partialRequestTime, parReqTime);


    // Send Request Message to the next stage
    sendRequestMessage("secondStage", requestId, threadId, "out");

}

// Handles a request that has completed all stages
void FirstStage::handleEnd(RequestMessage* msg) {

    // Debug Logging
    EV_DEBUG << "FirstStage::handleEnd called" << endl;

    // Extract Request ID, Thread ID and Logging
    long requestId = msg->getRequestId();
    int threadId = msg->getThreadId();
    EV_INFO << "Request " << requestId << " with Thread: " << threadId << " completed. Thread released." << endl;

    // Sending Request Completion to Client
    sendRequestMessage("end", requestId, threadId, "endReqOut");

    // If the queue is not empty extract a request and schedule it
    if (!waitingRequests.empty()) {
        long nextRequestId = waitingRequests.front();
        waitingRequests.pop();
        scheduleRequest(nextRequestId, threadId);
        EV_INFO << "Request " << nextRequestId << " extracted from queue and being served." << endl;
        emit(queueSize, waitingRequests.size());
    }

    // Otherwise increase the number of available threads
    else {
        availableThreads++;
        availableThreadIDs.push(threadId);
    }

}

// Main message handler
void FirstStage::handleMessage(cMessage* msg) {

    // Debug logging
    EV_DEBUG << "FirstStage::handleMessage called." << endl;

    auto* reqMsg = check_and_cast<RequestMessage*>(msg);

    // A request has arrived from a client
    if (msg->isName("serve"))
        handleServe(reqMsg);

    // A request has completed the first stage
    else if (msg->isName("secondStage"))
        handleSecondStage(reqMsg);

    // A completed request has arrived from second stage
    else if (msg->isName("end"))
        handleEnd(reqMsg);

    // If an unforeseen message arrives throw an error
    else
        throw cRuntimeError("FirstStage received an unknown message: '%s'", msg->getName());


    delete msg;

}
/*
void FirstStage::initialize() {

    numThreads = par("numThreads").intValue();
    meanServiceTime = par("meanServiceTime").doubleValue();
    availableThreads = numThreads;

}

void FirstStage::handleMessage(cMessage *msg)
{

    // A new request from one of the clients has arrived
    if (msg->isName("serve")) {

        // Cast the incoming message to the expected type and extract
        // request ID
        auto* requestMsg = check_and_cast<RequestMessage*>(msg);
        long requestId = requestMsg->getRequestId();

        // Log arrival of the request
        EV_INFO << "Request " << requestId << " arrived." << endl;

        // No Threads available: queue the request
        if (availableThreads == 0) {

            // Pushing to the Queue and Logging
            waitingRequests.push(requestId);
            EV_INFO << "Request " << requestId << " queued due to no available threads." << endl;

        }
        // Threads available: serve the request
        else {

            // Decrease Thread counter
            availableThreads--;

            // Create a self-message to signal end of first stage
            auto* endMsg = new RequestMessage("secondStage");
            endMsg->setRequestId(requestId);

            // Schedule the end of service after a random delay
            simtime_t delay = uniform(0, meanServiceTime);
            scheduleAt(simTime() + delay, endMsg);

            // Logging
            EV_INFO << "Request " << requestId << " is being served. Delay: " << delay << endl;
        }

        // Clean up the original message
        delete requestMsg;
    }

    // A request has completed this stage
    else if (msg->isName("secondStage")) {

        // Extracting Request ID from message and logging
        auto *tmpMsg = check_and_cast<RequestMessage*>(msg);
        long requestId = tmpMsg->getRequestId();
        EV_INFO << "Request " << requestId << " at the first stage has ended its execution, going to second stage" << endl;

        // Sending Request to the next Stage
        send(msg, "out");

    }

    // A request has been completed
    else if (msg->isName("end")) {

        // Extract request id and log
        auto* endMsg = check_and_cast<RequestMessage*>(msg);
        long requestId = endMsg->getRequestId();
        EV_INFO << "Request " << requestId << " completed. Thread released." << endl;

        // Creating and Sending End Message to Client
        auto* clientEndMsg = new RequestMessage("end");
        clientEndMsg->setRequestId(requestId);
        send(clientEndMsg, "endReqOut");
        EV_INFO << "Request Completion Message sent from First Stage to Clients. Request ID: " << requestId << endl;

        // Check if there are pending requests in the queue
        if (!waitingRequests.empty()) {

            // Extract the next request from the queue
            long requestId = waitingRequests.front();
            waitingRequests.pop();

            // Create a self-message to simulate serving of the extracted request
            auto* endMsg = new RequestMessage("secondStage");
            endMsg->setRequestId(requestId);

            // Schedule the end of service after a random delay
            simtime_t delay = uniform(0, meanServiceTime);
            scheduleAt(simTime() + delay, endMsg);

            EV_INFO << "Request " << requestId << " extracted from queue and being served. Delay: " << delay << endl;
        }
        // No queued requests: release a thread
        else {

            // Releasing thread
            availableThreads++;
        }

        // Clean up the processed message
        delete msg;
    }
    */


}; // namespace

