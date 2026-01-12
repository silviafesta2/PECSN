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

namespace project;


packet PipelineMessage {
    long requestId = 0;
    int clientId = 0;
    int threadId = 0;
    simtime_t arrivalFirst = SIMTIME_ZERO;
    simtime_t arrivalSecond = SIMTIME_ZERO;
    simtime_t arrivalThird = SIMTIME_ZERO;
}
