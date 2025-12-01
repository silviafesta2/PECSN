/*
 * Hub.h
 *
 *  Created on: Nov 5, 2025
 *      Author: simo
 */

#ifndef CLIENTSTAGE_H_
#define CLIENTSTAGE_H_

#include <omnetpp.h>
#include <unordered_map>
#include "ClientRequestMessage_m.h"
#include "RequestMessage_m.h"

using namespace omnetpp;

namespace project {

/**
 * Implements the Hub simple module. See the NED file for more information.
 */
class ClientStage : public cSimpleModule
{
  protected:
    virtual void initialize();
    virtual void handleMessage(cMessage *msg);
    virtual void scheduleNextRequest(int clientId);
    virtual void handleStartRequest(ClientRequestMessage* msg);
    virtual void handleEnd(RequestMessage* msg);

  private:
    int numClients;
    double requestMeanTime;
    long maxRequestId;
    std::unordered_map<long, int> requestId2clientId;
    std::unordered_map<long, simtime_t> requestId2time;
    simsignal_t requestTime;

};

}; // namespace

#endif
