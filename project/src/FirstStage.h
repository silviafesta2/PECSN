/*
 * Hub.h
 *
 *  Created on: Nov 5, 2025
 *      Author: simo
 */

#ifndef FIRSTSTAGE_H_
#define FIRSTSTAGE_H_

#include <omnetpp.h>
#include <queue>
#include "RequestMessage_m.h"

using namespace omnetpp;

namespace project {

/**
 * Implements the Hub simple module. See the NED file for more information.
 */
class FirstStage : public cSimpleModule
{
  protected:
    virtual void initialize();
    virtual void handleMessage(cMessage *msg);
    virtual simtime_t getServiceDelay(int threadId) const;
    virtual void scheduleRequest(long requestId, int threadId);
    virtual void sendRequestMessage(const char* name, long requestId, int threadId, const char* gateName);
    virtual void handleServe(RequestMessage* msg);
    virtual void handleSecondStage(RequestMessage* msg);
    virtual void handleEnd(RequestMessage* msg);

  private:
    int numThreads;
    int availableThreads;
    double meanServiceTime;
    std::queue<long> waitingRequests;
    std::queue<int> availableThreadIDs;
    std::unordered_map<long, simtime_t> requestId2arrivalTime;
    simsignal_t queueSize;
    simsignal_t partialRequestTime;
};

}; // namespace

#endif
