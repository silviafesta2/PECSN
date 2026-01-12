/*
 * Hub.h
 *
 *  Created on: Nov 5, 2025
 *      Author: simo
 */

#ifndef CLIENTSTAGE_H_
#define CLIENTSTAGE_H_


#include <omnetpp.h>
#include "PipelineMessage_m.h"


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
    virtual void handleClientRequest(PipelineMessage* msg);
  private:
    // Module Parameters
    int numClients;
    double requestMeanTime;
    // Counter of request ID's
    long maxRequestId;


};

} // namespace project

#endif // CLIENTSTAGE_H_
