#include "ThirdStage.h"
#include "RequestMessage_m.h"
#include "ClientRequestMessage_m.h"
#include <queue>

namespace project {

Define_Module(ThirdStage);

// Called once at the beginning of the simulation
void ThirdStage::initialize() {

    // Load service time parameter from NED file
    meanServiceTime = par("meanServiceTime").doubleValue();

}

// Computes a random service delay using a uniform distribution
simtime_t ThirdStage::getServiceDelay(int threadId) const {

    // Debug Logging
    EV_DEBUG << "ThirdStage::getServiceDelay called. threadId: " << threadId << endl;

    return uniform(0, 2 * meanServiceTime, threadId);
}

// Schedules the end of a request after a computed delay
void ThirdStage::scheduleExecution(long requestId, int threadId) {

    // Debug Logging
    EV_DEBUG << "ThirdStage:scheduleExecution called. requestId: " << requestId
             << ", threadId: " << threadId << endl;

    // Create Message, compute delay and send it to itself
    auto* endMsg = new RequestMessage("end");
    endMsg->setRequestId(requestId);
    endMsg->setThreadId(threadId);
    simtime_t delay = getServiceDelay(threadId);
    scheduleAt(simTime() + delay, endMsg);

    // Logging
    EV_INFO << "Request started execution. Execution Time: " << delay
            << ", Request ID: " << requestId << endl;

}

// Sends a request message to a specified gate
void ThirdStage::sendRequestMessage(const char* name, long requestId, int threadId, const char* gateName) {

    // Debug Logging
    EV_DEBUG << "ThirdStage::sendRequestMessage called. name: " << name << ", requestId: "
             << requestId << ", threadId: " << threadId << ", gateName" << gateName;

    // Create and Send Request Message
    auto* msg = new RequestMessage(name);
    msg->setRequestId(requestId);
    msg->setThreadId(threadId);
    send(msg, gateName);

    // Logging
    EV_INFO << "Sent message '" << name << "' to gate '" << gateName <<
            "'. Request ID: " << requestId << "Thread ID: " << threadId << endl;

}

// Handles a new request arriving from the second stage
void ThirdStage::handleIncomingRequest(RequestMessage* msg) {

    // Debug Logging
    EV_DEBUG << "ThirdStage::handleIncomingRequest called" << endl;

    // Extract Request ID from Messages
    long requestId = msg->getRequestId();
    int threadId = msg->getThreadId();
    EV_INFO << "Request arrived at third stage. Request ID: " << requestId
            << " Thread ID: " << threadId << endl;

    //
    // The requests cannot queue up as they can be, at most, equal to
    // the number of available threads but not greater than it (first
    // stage is queuing the requests in excess)
    //
    // Schedule execution of the request
    //
    scheduleExecution(requestId, threadId);
}

// Handles a completed request and forwards it to the second stage
void ThirdStage::handleCompletedRequest(RequestMessage* msg) {

    // Debug Logging
    EV_DEBUG << "ThirdStage::handleCompletedRequest called." << endl;

    // Extract Request ID from Message
    long requestId = msg->getRequestId();
    int threadId = msg->getThreadId();
    EV_INFO << "Request completed third stage. Request ID: " << requestId
            << "Thread ID: " << threadId << endl;

    // Forward completion message to second stage
    sendRequestMessage("end", requestId, threadId, "out");
}

// Main message handler
void ThirdStage::handleMessage(cMessage* msg) {

    // Debug Logging
    EV_DEBUG << "ThirdStage:handleMessage called." << endl;

    auto* reqMsg = check_and_cast<RequestMessage*>(msg);

    // A request has been forwarded from the Second Stage
    if (msg->isName("endSecondStage"))
        handleIncomingRequest(reqMsg);

    // A request has completed the Third Stage
    else if (msg->isName("end"))
        handleCompletedRequest(reqMsg);

    // If an unforeseen message arrives throw an error
    else
        throw cRuntimeError("ThirdStage received an unknown message: '%s'", msg->getName());


    delete msg;
}

/*
void ThirdStage::initialize() {

    meanServiceTime = par("meanServiceTime").doubleValue();

}

void ThirdStage::handleMessage(cMessage *msg)
{

    // A request has been forwarded from the previous stage
    if (msg->isName("endSecondStage")) {

        // Extracting Request ID and Logging
        auto *tmpMsg = check_and_cast<RequestMessage*>(msg);
        long requestId = tmpMsg->getRequestId();
        EV_INFO << "A Request has arrived to third stage. Request ID: " << requestId << endl;

        //
        // The requests cannot queue up as they can be, at most, equal to
        // the number of available threads but not greater than it (first
        // stage is queuing the requests in excess)
        //
        // Executing the request (computing the execution time and sending
        // a timer for the conclusion of the request) and Logging
        //
        auto* endMsg = new RequestMessage("end");
        endMsg->setRequestId(requestId);
        simtime_t delay = uniform(0, 2 * meanServiceTime);
        scheduleAt(simTime() + delay, endMsg);
        EV_INFO << "Request has started its execution. Execution Time: " << delay;
        EV_INFO << ". Request ID: " << requestId << endl;

    }

    // A request has ended its execution
    else if (msg->isName("end")) {

        // Extracting Request ID and Logging
        auto *tmpMsg = check_and_cast<RequestMessage*>(msg);
        long requestId = tmpMsg->getRequestId();
        EV_INFO << "A request has completed the third stage. Request ID: " << requestId << endl;

        // Sending Message of completion of the request to the Second
        // Stage and Logging
        auto* endMsg = new RequestMessage("end");
        endMsg->setRequestId(requestId);
        send(endMsg, "out");
        EV_INFO << "Completed Request has been sent to the second stage. Request ID: ";
        EV_INFO << requestId << endl;

    }

    delete msg;

}
*/

}; // namespace
