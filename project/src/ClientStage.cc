#include "ClientStage.h"


namespace project {

Define_Module(ClientStage);

// Initialization: set up parameters and schedule each client's first request
void ClientStage::initialize() {

    // Load parameters from NED file
    numClients = par("numClients").intValue();
    requestMeanTime = par("requestMeanTime").doubleValue();

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

    // Create new PipelineMessage
    PipelineMessage* reqMsg = new PipelineMessage("clientRequest");
    reqMsg->setClientId(clientId);
    reqMsg->setRequestId(maxRequestId++);

    // Compute delay using exponential distribution with client-specific RNG
    simtime_t delay = exponential(requestMeanTime, clientId);
    scheduleAt(simTime() + delay, reqMsg);

}

// Handler for clientRequest
void ClientStage::handleClientRequest(PipelineMessage* msg) {

    // Debug Logging
    EV_DEBUG << "ClientStage::handleClientRequest called" << endl;

    // Info Logging
    EV_INFO << "Sending request for client " << msg->getClientId()
            << ", request ID: " << msg->getRequestId() << endl;

    // Update request name and send to next stage
    msg->setName("toServe1");
    send(msg, "out");

    // Schedule the next request for this client
    scheduleNextRequest(msg->getClientId());

}

// Main message handler
void ClientStage::handleMessage(cMessage* msg) {

    // Debug Logging
    EV_DEBUG << "ClientStage::handleMessage called." << endl;

    // A request has arrived to ClientStage
    // Check message name and call the proper handler
    if (msg->isName("clientRequest")) {
        PipelineMessage* reqMsg = check_and_cast<PipelineMessage*>(msg);
        handleClientRequest(reqMsg);
    }

    // If an unforeseen message arrives throw an error
    else
        throw cRuntimeError("ClientStage received an unknown message: '%s'", msg->getName());
}

};
