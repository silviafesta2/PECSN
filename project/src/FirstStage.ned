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

package project;

//
// Immediately sends out any message it receives. It can optionally generate
// a message at the beginning of the simulation, to bootstrap the process.
//

simple FirstStage
{
    parameters:
        int numThreads = default(2);
        double meanServiceTime = default(10);
        @signal[queueSize];
		@statistic[queueSize](source=queueSize; record=vector, mean, max, timeavg);
		@signal[partialRequestTime];
		@statistic[partialRequestTime](source=partialRequestTime; record=vector);
		

    gates:
        input in;
        input endReqIn;
        output out;
        output endReqOut;
}