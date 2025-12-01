#include "SecondStage.h"
#include "RequestMessage_m.h"
#include "ClientRequestMessage_m.h"
#include <queue>

namespace project {

Define_Module(SecondStage);


// Called once at the beginning of the simulation
void SecondStage::initialize() {

    // Load parameters from NED file
    meanServiceTime = par("meanServiceTime").doubleValue();
    EV_INFO << "aaa" << meanServiceTime << endl;
    lognormalServiceTime = par("lognormalServiceTime").boolValue();
    stdServiceTime = par("stdServiceTime").doubleValue();

    // Registering Signal
    queueSize = registerSignal("queueSize");
    partialRequestTime = registerSignal("partialRequestTime");
    emit(queueSize, 0);

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

// Schedules the end of a request after a delay
void SecondStage::scheduleRequest(long requestId, int threadId) {

    // Debug Logging
    EV_DEBUG << "SecondStage::scheduleRequest called. requestId: " << requestId
             << ", threadId: " << threadId;

    // Create a new message to signal the end of processing
    auto* endMsg = new RequestMessage("endSecondStage");
    endMsg->setRequestId(requestId);
    endMsg->setThreadId(threadId);

    // Compute delay and schedule the message
    simtime_t delay = getServiceDelay(threadId);
    scheduleAt(simTime() + delay, endMsg);

    // Log the scheduling
    EV_INFO << "Scheduled request. Request ID: " << requestId << "Thread ID: " <<
            threadId << ", serviceTime: " << delay << endl;
}

// Sends a RequestMessage with a given name and request ID through a specified gate
void SecondStage::sendRequestMessage(const char* name, long requestId, int threadId, const char* gateName) {

    // Debug Logging
    EV_DEBUG << "SecondStage::sendRequestMessage called. name: " << name << ", requestId: "
             << requestId << ", threadId: " << threadId << ", gateName: " << gateName;

    // Creating and Sending Request Message
    auto* msg = new RequestMessage(name);
    msg->setRequestId(requestId);
    msg->setThreadId(threadId);
    send(msg, gateName);

    // Log the sending
    EV_INFO << "Sent message '" << name << "' to gate '" << gateName
            << "'. Request ID: " << requestId << " Thread ID: " << threadId << endl;
}

// Handles a new request arriving at the second stage
void SecondStage::handleSecondStage(RequestMessage* msg) {

    // Debug Logging
    EV_DEBUG << "SecondStage::handleSecondStage called." << endl;

    // Extracting Request ID and Logging
    long requestId = msg->getRequestId();
    int threadId = msg->getThreadId();
    EV_INFO << "Request arrived at second stage. Request ID: " << requestId;
    EV_INFO << " Thread ID: " << threadId << endl;

    // Stores arrival time of the request in an unordered map (which maps
    // request ID to arrival times) that will be used to compute partial
    // request time
    requestId2arrivalTime[requestId] = simTime();

    // The lock is already taken, queue the request and log
    if (lock) {
        waitingRequests.push(requestId);
        waitingRequest2ThreadId[requestId] = threadId;
        EV_INFO << "Lock taken, queuing request. Request ID: " << requestId;
        EV_INFO << " Thread ID: " << threadId << endl;
        emit(queueSize, waitingRequests.size());
    }
    // Otherwise take the lock and schedule the end of the request
    else {
        lock = true;
        scheduleRequest(requestId, threadId);
    }
}

// Handles the completion of a request at the second stage
void SecondStage::handleEndSecondStage(RequestMessage* msg) {

    // Debug Logging
    EV_DEBUG << "SecondStage::handleEndSecondStage called." << endl;

    // Extracting Request ID and Logging
    long requestId = msg->getRequestId();
    int threadId = msg->getThreadId();
    EV_INFO << "Request completed at second stage. Releasing lock. Request ID: " << requestId;
    EV_INFO << " Thread ID: " << threadId << endl;

    // Compute Partial Request Time by extracting arrival time from an hash map
    // write the statistics and remove entry from hash map
    simtime_t parReqTime = simTime() - requestId2arrivalTime[requestId];
    requestId2arrivalTime.erase(requestId);
    emit(partialRequestTime, parReqTime);

    // If there are queued requests, process the next one
    if (!waitingRequests.empty()) {
        long nextRequestId = waitingRequests.front();
        waitingRequests.pop();
        int nextThreadId = waitingRequest2ThreadId[nextRequestId];
        waitingRequest2ThreadId.erase(nextRequestId);
        scheduleRequest(nextRequestId, nextThreadId);
        emit(queueSize, waitingRequests.size());
    }
    // Otherwise, release the lock
    else {
        lock = false;
        EV_INFO << "No queued requests. Lock released. Request ID: " << requestId;
        EV_INFO << " Thread ID: " << threadId << endl;
    }

    //  Forwarding to the third (last) processing stage
    sendRequestMessage("endSecondStage", requestId, threadId, "out");
}

// Handles a completed request arriving from the third stage
void SecondStage::handleEnd(RequestMessage* msg) {

    // Debug Logging
    EV_DEBUG << "SecondStage::handleEnd called." << endl;

    // Extracting Request ID and Logging
    long requestId = msg->getRequestId();
    int threadId = msg->getThreadId();
    EV_INFO << "Completed request arrived from third stage. Request ID: " << requestId;
    EV_INFO << " Thread ID: " << threadId << endl;

    // Forward the completion message to the first stage
    sendRequestMessage("end", requestId, threadId, "endReqOut");
}

// Main message handler
void SecondStage::handleMessage(cMessage* msg) {

    // Debug Logging
    EV_DEBUG << "SecondStage::handleMessage called." << endl;

    // Cast to our custom message type
    auto* reqMsg = check_and_cast<RequestMessage*>(msg);

    // A request has been forwarded from the first stage
    if (msg->isName("secondStage"))
        handleSecondStage(reqMsg);

    // A scheduled request has ended its execution
    else if (msg->isName("endSecondStage"))
        handleEndSecondStage(reqMsg);

    // A completed request has been send from the third stage
    else if (msg->isName("end"))
        handleEnd(reqMsg);

    // If an unforeseen message arrives throw an error
    else
        throw cRuntimeError("SecondStage received an unknown message: '%s'", msg->getName());


    // Clean up the message
    delete msg;
}

/*

void SecondStage::initialize() {

    meanServiceTime = par("meanServiceTime").doubleValue();
    lognormalServiceTime = par("lognormalServiceTime").boolValue();
    stdServiceTime = par("stdServiceTime").doubleValue();
    lock = false;

}

void SecondStage::handleMessage(cMessage *msg)
{

    // A request has been forwarded from the previous stage
    if (msg->isName("secondStage")) {

        // Extracting Request ID and Logging
        auto* tmpMsg = check_and_cast<RequestMessage*>(msg);
        long requestId = tmpMsg->getRequestId();
        EV_INFO << "New request has arrived to the second stage. Request ID: " << requestId << endl;

        // If the lock is already taken then wait in a FIFO queue
        if (lock) {

            // Inserting at the bottom of the queue and logging
            waitingRequests.push(requestId);
            EV_INFO << "The lock is already taken, request being queued. Request ID: " << requestId << endl;

        }

        // If the lock is not taken then take it and schedule end of this request
        else {

            // Taking the lock
            lock = true;

            // Creating message for the end of this request
            auto* endMsg = new RequestMessage("endSecondStage");
            endMsg->setRequestId(requestId);

            // Scheduling End of the request using a uniform or lognormal distribution
            simtime_t delay;
            if (lognormalServiceTime)
                delay = uniform(0, 2 * meanServiceTime);
            else
                delay = lognormal(meanServiceTime, stdServiceTime);
            scheduleAt(simTime() + delay, endMsg);

            // Logging
            EV_INFO << "The lock has been taken, serviceTime: " << delay << ". Request ID: " << requestId << endl;

        }

    }

    // A request has completed the stage
    else if (msg->isName("endSecondStage")) {

        // Extracting requestId and logging
        auto* tmpMsg = check_and_cast<RequestMessage*>(msg);
        long requestId = tmpMsg->getRequestId();
        EV_INFO << "Request has completed second stage, releasing lock. Request ID: " << requestId << endl;

        // If a request is waiting to acquire the lock extract it from the queue and schedule it
        if (waitingRequests.size() > 0) {

            // Extracting and removing first request
            long newRequestId = waitingRequests.front();
            waitingRequests.pop();

            // Creating message for the end of this request
            auto* endMsg = new RequestMessage("endSecondStage");
            endMsg->setRequestId(newRequestId);

            // Scheduling End of the request using a uniform distribution
            simtime_t delay;
            if (lognormalServiceTime)
                delay = uniform(0, 2 * meanServiceTime);
            else
                delay = lognormal(meanServiceTime, stdServiceTime);
            scheduleAt(simTime() + delay, endMsg);

            // Logging
            EV_INFO << "The lock has been taken, serviceTime: " << delay << ". Request ID: " << newRequestId << endl;

        }

        // If there aren't any queued requests release the lock
        else {

            // Releasing the lock and logging
            lock = false;
            EV_INFO << "The lock has been released. Request Id: " << requestId << endl;

        }

        // Creating and Sending Message to the First Stage
        auto* endMsg = new RequestMessage("endSecondStage");
        endMsg->setRequestId(requestId);
        send(endMsg, "out");

        // Logging
        EV_INFO << "The message of completion of the request has been sent from second stage to first state. Request ID: " << requestId << endl;

    }

    // A completed request has been sent from the third stage
    else if (msg->isName("end")) {

        // Extract Request ID and Logging
        auto* tmpMsg = check_and_cast<RequestMessage*>(msg);
        long requestId = tmpMsg->getRequestId();
        EV_INFO << "A Completed Requested has arrived to the Second Stage. Request ID: " << requestId << endl;

        // Sending Completion Message to First Stage
        auto* endMsg = new RequestMessage("end");
        endMsg->setRequestId(requestId);
        send(endMsg, "endReqOut");
        EV_INFO << "Completed Request has been sent to the First Stage. Request ID: " << requestId << endl;

    }

    delete msg;

}

*/

}; // namespace
