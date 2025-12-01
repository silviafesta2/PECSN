//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program.  If not, see http://www.gnu.org/licenses/.
// 

#ifndef SECONDSTAGE_H_
#define SECONDSTAGE_H_

#include <omnetpp.h>
#include <queue>
#include <map>
#include "RequestMessage_m.h"

using namespace omnetpp;

namespace project {


class SecondStage : public cSimpleModule
{
  protected:
    virtual void initialize();
    virtual void handleMessage(cMessage *msg);
    virtual void handleEnd(RequestMessage* msg);
    virtual void handleEndSecondStage(RequestMessage* msg);
    virtual void handleSecondStage(RequestMessage* msg);
    virtual void sendRequestMessage(const char* name, long requestId, int threadId, const char* gateName);
    virtual void scheduleRequest(long requestId, int threadId);
    virtual simtime_t getServiceDelay(int threadId) const;

  private:
    double meanServiceTime;
    double stdServiceTime;
    bool lognormalServiceTime;
    bool lock;
    std::map<long, int> waitingRequest2ThreadId;
    std::queue<double> waitingRequests;
    std::unordered_map<long, simtime_t> requestId2arrivalTime;
    simsignal_t queueSize;
    simsignal_t partialRequestTime;
};


}; // namespace

#endif

