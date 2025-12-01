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

package project.simulations;

import project.ClientStage;
import project.FirstStage;
import project.SecondStage;
import project.ThirdStage;


//
// Two instances (tic and toc) of Txc connected.
//
network Pipeline
{
    @display("bgb=356,109");
    submodules:
        clients: ClientStage;
        firstStage: FirstStage;
        secondStage: SecondStage;
        thirdStage: ThirdStage;

    connections:
        clients.out --> firstStage.in;
        firstStage.out --> secondStage.in;
        secondStage.out --> thirdStage.in;
        thirdStage.out --> secondStage.endReqIn;
        secondStage.endReqOut --> firstStage.endReqIn;
        firstStage.endReqOut --> clients.in;

}