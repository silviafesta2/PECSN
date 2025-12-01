#include "ClientStage.h"
#include "ClientRequestMessage_m.h"
#include "RequestMessage_m.h"


namespace project {

Define_Module(ClientStage);

// Initialization: set up clients and schedule their first requests
void ClientStage::initialize() {

    // Load parameters from NED file
    numClients = par("numClients").intValue();
    requestMeanTime = par("requestMeanTime").doubleValue();

    // Register signal for tracking request durations
    requestTime = registerSignal("requestTime");

    // Initialize request ID counter
    maxRequestId = 0;

    // Schedule the first request for each client
    for (int clientId = 0; clientId < numClients; ++clientId) {
        scheduleNextRequest(clientId);
    }
}

// Schedules the next request for a given client
void ClientStage::scheduleNextRequest(int clientId) {

    // Debug Logging
    EV_DEBUG << "ClientStage::scheduleNextRequest called. clientId: " << clientId << endl;

    // Create ClientRequest Message
    auto* reqMsg = new ClientRequestMessage("startRequest");
    reqMsg->setClientId(clientId);
    reqMsg->setRequestId(maxRequestId);

    // Track client ID
    requestId2clientId[maxRequestId] = clientId;

    // Compute delay using exponential distribution with client-specific RNG
    simtime_t delay = exponential(requestMeanTime, clientId);
    scheduleAt(simTime() + delay, reqMsg);

    maxRequestId++;
}

// Handles a request initiation message
void ClientStage::handleStartRequest(ClientRequestMessage* msg) {

    // Debug Logging
    EV_DEBUG << "ClientStage::handleStartRequest called" << endl;

    // Extract Client ID and Request ID + Logging
    int clientId = msg->getClientId();
    long requestId = msg->getRequestId();
    EV_INFO << "Sending request for client " << clientId
            << ", request ID: " << requestId << endl;

    // Send request to first stage
    auto* newMsg = new RequestMessage("serve");
    newMsg->setRequestId(requestId);
    send(newMsg, "out");
    requestId2time[requestId] = simTime();

    // Schedule the next request for this client
    scheduleNextRequest(clientId);

}

// Handles a completed request message
void ClientStage::handleEnd(RequestMessage* msg) {

    // Debug Logging
    EV_DEBUG << "ClientStage::handleEnd called" << endl;

    // Extract ID
    long requestId = msg->getRequestId();

    // Retrieve and remove client ID mapping
    int clientId = requestId2clientId[requestId];
    requestId2clientId.erase(requestId);

    // Compute and emit request duration
    simtime_t reqTime = simTime() - requestId2time[requestId];
    requestId2time.erase(requestId);
    emit(requestTime, reqTime);

    EV_INFO << "ClientStage received completion of request ID " << requestId
            << " from client " << clientId << ". Duration: " << reqTime << endl;

}

// Main message handler
void ClientStage::handleMessage(cMessage* msg) {

    // Debug Logging
    EV_DEBUG << "ClientStage::handleMessage called." << endl;

    // A scheduled request has arrived
    // Convert Message and call the handler
    if (msg->isName("startRequest")) {
        auto* reqMsg = check_and_cast<ClientRequestMessage*>(msg);
        handleStartRequest(reqMsg);
    }

    // A completed request has arrived
    // Convert Message and call the Handler
    else if (msg->isName("end")) {
        auto* reqMsg = check_and_cast<RequestMessage*>(msg);
        handleEnd(reqMsg);
    }

    // If an unforeseen message arrives throw an error
    else
        throw cRuntimeError("SecondStage received an unknown message: '%s'", msg->getName());


    delete msg;
}

/*
// Schedules the requests for each client
void ClientStage::initialize() {

    // Reading Parameters
    numClients = par("numClients").intValue();
    requestMeanTime = par("requestMeanTime").doubleValue();

    // Registering Signals
    requestTime = registerSignal("requestTime");

    // Request IDs start from zero
    maxRequestId = 0;

    // For each client schedule the first request
    for (int clientId = 0; clientId < numClients; ++clientId) {

        // Creating the message
        auto* reqMsg = new ClientRequestMessage("startRequest");
        reqMsg->setClientId(clientId);
        reqMsg->setRequestId(maxRequestId);

        // Updating Request to Client map
        requestId2clientId[maxRequestId] = clientId;
        maxRequestId += 1;

        // Computing delay drawing from an exponential distribution and using
        // a different local RNG for each client
        simtime_t delay = exponential(requestMeanTime, clientId);
        scheduleAt(simTime() + delay, reqMsg);
    }
}

void ClientStage::handleMessage(cMessage* msg) {

    // A request has to be sent
    if (msg->isName("startRequest")) {

        // Reading the Id of the client
        auto* tmpMsg = check_and_cast<ClientRequestMessage*>(msg);
        int clientId = tmpMsg->getClientId();
        long requestId = tmpMsg->getRequestId();

        // Sending the request to the next stage
        EV_INFO << "Sending request for client " << clientId << " requestId" << requestId << endl;
        auto* newMsg = new RequestMessage("serve");
        newMsg->setRequestId(requestId);
        send(newMsg, "out");

        // Scheduling next request
        auto* reqMsg = new ClientRequestMessage("startRequest");
        reqMsg->setRequestId(maxRequestId);
        reqMsg->setClientId(clientId);
        simtime_t delay = exponential(requestMeanTime, clientId);
        scheduleAt(simTime() + delay, reqMsg);

        // Updating Hash Map (Request ID --> Client ID, Time)
        requestId2clientId[maxRequestId] = clientId;
        requestId2time[maxRequestId] = simTime();
        maxRequestId += 1;

    }

    // A request has been completed
    else if (msg->isName("end")) {

        // Reading the Id of the request from the Message
        auto* reqMsg = check_and_cast<RequestMessage*>(msg);
        long requestId = reqMsg->getRequestId();

        // Extracting the Client ID using the hash map populated when sending requests
        // and also removing the Request ID key as it won't be used anymore (saving space)
        int clientId = requestId2clientId[requestId];
        requestId2clientId.erase(requestId);

        // Writing request time with a signal and updating the hash map
        simtime_t reqTime = simTime() - requestId2time[requestId];
        requestId2time.erase(requestId);
        emit(requestTime, reqTime);

        // Logging
        EV_INFO << "Client Stage has received the completion of a request. Request ID: ";
        EV_INFO << requestId << ", clientID" << clientId << endl;

    }

    else {
        EV_WARN << "Unexpected message: " << msg->getName() << endl;
    }

    delete msg;
}
*/

}; // namespace

