#include "SecondStage.h"

namespace project {

Define_Module(SecondStage);


// Called once at the beginning of the simulation
void SecondStage::initialize() {

    // Load parameters from NED file
    meanServiceTime = par("meanServiceTime").doubleValue();
    lognormalServiceTime = par("lognormalServiceTime").boolValue();
    stdServiceTime = par("stdServiceTime").doubleValue();


    // Registering Signal
    queueSize2 = registerSignal("queueSize2");
    partialResponseTime2 = registerSignal("partialResponseTime2");
    emit(queueSize2, 0);

    // Lock indicates whether the stage is currently processing a request
    lock = false;
}

// Returns a service delay based on the configured distribution
simtime_t SecondStage::getServiceDelay(int threadId) const {

    // Debug Logging
    EV_DEBUG << "SecondStage::getServiceDelay called. threadId: " << threadId << endl;

    // Use log normal or uniform distribution depending on the parameter
    return lognormalServiceTime ? lognormal(meanServiceTime, stdServiceTime, threadId)
                                : uniform(0, 2 * meanServiceTime, threadId);

}

// Main message handler
void SecondStage::handleMessage(cMessage* msg) {

    // Debug Logging
    EV_DEBUG << "SecondStage::handleMessage called." << endl;

    // A request has been forwarded from the first stage
    if (msg->isName("toServe2")) {
        handleServe2(check_and_cast<PipelineMessage*>(msg));
        return; // ownership handled inside
    }

    // A request has completed its processing in second stage
    else if (msg->isName("toServe3")) {
        handleSendToThirdStage(check_and_cast<PipelineMessage*>(msg));
        return;
    }

    // If an unforeseen message arrives throw an error
    else
        throw cRuntimeError("SecondStage received an unknown message: '%s'", msg->getName());

}


// Handles a new request arriving at the second stage
void SecondStage::handleServe2(PipelineMessage* msg) {

    long requestId = msg->getRequestId();
    int threadId = msg->getThreadId();

    // Debug Logging
    EV_DEBUG << "SecondStage::handleServe2 called." << endl;

    // Info Logging
    EV_INFO << "Request arrived at second stage. Request ID: " << requestId
            << " Thread ID: " << threadId << endl;

    // Store arrival time at this stage inside the message
    msg->setArrivalSecond(simTime());

    // The lock is already taken, queue the request and log
    if (lock) {
        waitingRequests.insert(msg); //insert in FIFO queue
        EV_INFO << "Lock already taken, queuing request. Request ID: " << requestId
                << " Thread ID: " << threadId << endl;
        emit(queueSize2, waitingRequests.getLength());
        return;
    }
    // Otherwise take the lock and schedule the completion of the request
    lock = true;
    scheduleSecondStageProcessingCompletion(msg); // scheduleRequest deletes msg
}

// Schedules the completion of a request after a delay
void SecondStage::scheduleSecondStageProcessingCompletion(PipelineMessage* srcMsg) {

    long requestId = srcMsg->getRequestId();
    int clientId = srcMsg->getClientId();
    int threadId = srcMsg->getThreadId();

    // Debug Logging
    EV_DEBUG << "SecondStage::scheduleRequest called. requestId: " << requestId
             << ", threadId: " << threadId << ", clientId: " << clientId << endl;

    // Schedule completion event using the same message
    srcMsg->setName("toServe3");
    simtime_t delay = getServiceDelay(threadId);
    scheduleAt(simTime() + delay, srcMsg);

    // Log the scheduling
    EV_INFO << "Scheduled request. Request ID: " << requestId << " Thread ID: " <<
            threadId << ", serviceTime: " << delay << endl;
}


// Handles the completion of a request at the second stage
void SecondStage::handleSendToThirdStage(PipelineMessage* msg) {

    long requestId = msg->getRequestId();
    int threadId = msg->getThreadId();

    // Debug Logging
    EV_DEBUG << "SecondStage::handleSendToThirdStage called." << endl;


    EV_INFO << "Request completed at second stage. Releasing lock. Request ID: " << requestId
            << " Thread ID: " << threadId << endl;

    // Compute Partial Request Time using arrival time stored in the message
    simtime_t parRespTime = simTime() - msg->getArrivalSecond();
    emit(partialResponseTime2, parRespTime);

    // If there are queued requests, process the next one
    if (!waitingRequests.isEmpty()) {
        PipelineMessage* nextMsg = check_and_cast<PipelineMessage*>(waitingRequests.pop());
        scheduleSecondStageProcessingCompletion(nextMsg);
        emit(queueSize2, waitingRequests.getLength());
        EV_INFO << "Second stage lock taken, processing new request. Request ID: " << nextMsg->getRequestId() << endl;
    }
    // Otherwise, release the lock
    else {
        lock = false;
        EV_INFO << "No queued requests. Lock released. Request ID: " << requestId
                << " Thread ID: " << threadId << endl;
    }

    //  Name already changed, forwarding to the third (last) processing stage
    send(msg, "out");
}


}; // namespace
