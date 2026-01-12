#include "ThirdStage.h"

namespace project {

Define_Module(ThirdStage);

// Called once at the beginning of the simulation
void ThirdStage::initialize() {

    // Load parameter from NED file
    meanServiceTime = par("meanServiceTime").doubleValue();

}

// Computes a random service delay using a uniform distribution
simtime_t ThirdStage::getServiceDelay(int threadId) const {

    // Debug Logging
    EV_DEBUG << "ThirdStage::getServiceDelay called. threadId: " << threadId << endl;

    return uniform(0, 2 * meanServiceTime, threadId);
}

// Main message handler
void ThirdStage::handleMessage(cMessage* msg) {

    // Debug Logging
    EV_DEBUG << "ThirdStage::handleMessage called." << endl;

    // A request has been forwarded from the Second Stage
    if (msg->isName("toServe3")) {
        auto* reqMsg = check_and_cast<PipelineMessage*>(msg);
        handleServe3(reqMsg);
        return; // ownership handled
    }

    // A request has completed the Third Stage
    else if (msg->isName("processingComplete")) {
        auto* reqMsg = check_and_cast<PipelineMessage*>(msg);
        handleSendBackToFirstStage(reqMsg);
        return;
    }

    // If an unforeseen message arrives throw an error
    else
        throw cRuntimeError("ThirdStage received an unknown message: '%s'", msg->getName());


}



// Handles a new request arriving from the second stage
void ThirdStage::handleServe3(PipelineMessage* msg) {

    // Debug Logging
    EV_DEBUG << "ThirdStage::handleServe3 called" << endl;

    //Info Logging
    EV_INFO << "Request arrived at third stage. Request ID: " << msg->getRequestId()
            << " Thread ID: " << msg->getThreadId() << endl;

    // Store arrival time at third stage inside the message
    msg->setArrivalThird(simTime());

    // No queuing needed at this stage
    // Schedule execution of the request
    scheduleThirdStageProcessingCompletion(msg);
}

// Schedules the completion of a request after a computed delay
void ThirdStage::scheduleThirdStageProcessingCompletion(PipelineMessage* srcMsg) {

    long requestId = srcMsg->getRequestId();
    int threadId = srcMsg->getThreadId();
    int clientId = srcMsg->getClientId();

    // Debug Logging
    EV_DEBUG << "ThirdStage::scheduleThirdStageProcessingCompletion called. requestId: " << requestId
             << ", threadId: " << threadId << ", clientId: " << clientId << endl;

    // Compute delay and schedule completion using the same message
    srcMsg->setName("processingComplete");
    simtime_t delay = getServiceDelay(threadId);
    scheduleAt(simTime() + delay, srcMsg);

    // Logging
    EV_INFO << "Request started execution. Execution Time: " << delay
            << ", Request ID: " << requestId << endl;

}


// Handles a completed request and sends it back to first stage
void ThirdStage::handleSendBackToFirstStage(PipelineMessage* msg) {

    // Debug Logging
    EV_DEBUG << "ThirdStage::handleSendBackToFirstStage called." << endl;

    // Info Logging
    EV_INFO << "Request completed third stage. Request ID: " << msg->getRequestId()
            << " Thread ID: " << msg->getThreadId() << endl;

    // send the message back to first stage
    send(msg, "out");
}


};
