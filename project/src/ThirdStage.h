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

#ifndef THIRDSTAGE_H_
#define THIRDSTAGE_H_

#include <omnetpp.h>
#include <queue>

#include "RequestMessage_m.h"

using namespace omnetpp;

namespace project {

class ThirdStage : public cSimpleModule
{
  protected:
    virtual void initialize();
    virtual void handleMessage(cMessage *msg);
    virtual void handleCompletedRequest(RequestMessage* msg);
    virtual void handleIncomingRequest(RequestMessage* msg);
    virtual void sendRequestMessage(const char* name, long requestId, int threadId, const char* gateName);
    virtual void scheduleExecution(long requestId, int threadId);
    virtual simtime_t getServiceDelay(int threadId) const;

  private:
    double meanServiceTime;

};

}; // namespace

#endif
