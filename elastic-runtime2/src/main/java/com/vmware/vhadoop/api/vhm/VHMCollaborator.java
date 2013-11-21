/***************************************************************************
* Copyright (c) 2013 VMware, Inc. All Rights Reserved.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
***************************************************************************/
package com.vmware.vhadoop.api.vhm;

/**
 * Marker interface for any component that's a general purpose VHMCollaborator
 * There are some components that implement many different interfaces which need to be registered with VHM
 *   Example interfaces are ClusterMapReader, VMChooser, EventInjector etc.
 * Rather than have multiple register calls for all the different types, there is a single call VHM.registerCollaborator()
 *   which will ensure that the component is appropriately hooked in to at all the right points
 * This interface should only ever be extended by other interfaces, not implemented directly by classes
 */
public interface VHMCollaborator {

}
