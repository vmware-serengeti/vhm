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

package com.vmware.vhadoop.vhm.vc;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLException;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.VCActions.VMEventData;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.ExternalizedParameters;
import com.vmware.vhadoop.util.LogFormatter;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.util.VhmLevel;
import com.vmware.vim.binding.impl.vim.event.EventExImpl;
import com.vmware.vim.binding.impl.vmodl.TypeNameImpl;
import com.vmware.vim.binding.vim.Folder;
import com.vmware.vim.binding.vim.PerformanceManager;
import com.vmware.vim.binding.vim.ServiceInstance;
import com.vmware.vim.binding.vim.ServiceInstanceContent;
import com.vmware.vim.binding.vim.SessionManager;
import com.vmware.vim.binding.vim.Task;
import com.vmware.vim.binding.vim.TaskInfo;
import com.vmware.vim.binding.vim.VirtualMachine;
import com.vmware.vim.binding.vim.VirtualMachine.PowerState;
import com.vmware.vim.binding.vim.alarm.Alarm;
import com.vmware.vim.binding.vim.alarm.AlarmManager;
import com.vmware.vim.binding.vim.event.Event.EventSeverity;
import com.vmware.vim.binding.vim.event.EventEx;
import com.vmware.vim.binding.vim.event.EventManager;
import com.vmware.vim.binding.vim.fault.HostConnectFault;
import com.vmware.vim.binding.vim.fault.InvalidEvent;
import com.vmware.vim.binding.vim.fault.VimFault;
import com.vmware.vim.binding.vim.net.IpConfigInfo;
import com.vmware.vim.binding.vim.net.IpConfigInfo.IpAddress;
import com.vmware.vim.binding.vim.option.OptionValue;
import com.vmware.vim.binding.vim.version.version8;
import com.vmware.vim.binding.vim.view.ContainerView;
import com.vmware.vim.binding.vim.view.ViewManager;
import com.vmware.vim.binding.vim.vm.GuestInfo.NicInfo;
import com.vmware.vim.binding.vmodl.DynamicProperty;
import com.vmware.vim.binding.vmodl.ManagedObjectReference;
import com.vmware.vim.binding.vmodl.TypeName;
import com.vmware.vim.binding.vmodl.fault.RequestCanceled;
import com.vmware.vim.binding.vmodl.query.InvalidCollectorVersion;
import com.vmware.vim.binding.vmodl.query.InvalidProperty;
import com.vmware.vim.binding.vmodl.query.PropertyCollector;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.Change;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.Filter;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.FilterSpec;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.FilterUpdate;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.ObjectContent;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.ObjectSpec;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.ObjectUpdate;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.ObjectUpdate.Kind;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.PropertySpec;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.RetrieveOptions;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.RetrieveResult;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.SelectionSpec;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.TraversalSpec;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.UpdateSet;
import com.vmware.vim.binding.vmodl.query.PropertyCollector.WaitOptions;
import com.vmware.vim.vmomi.client.Client;
import com.vmware.vim.vmomi.client.exception.ConnectionException;
import com.vmware.vim.vmomi.client.exception.TransportProtocolException;
import com.vmware.vim.vmomi.client.http.HttpClientConfiguration;
import com.vmware.vim.vmomi.client.http.ThumbprintVerifier;
import com.vmware.vim.vmomi.client.http.impl.HttpConfigurationImpl;
import com.vmware.vim.vmomi.core.types.VmodlContext;

public class VcVlsi {

   public static final String SERENGETI_MASTERVM_NAME_POSTFIX = "-master-";
   public static final String SERENGETI_MASTERVM_NAME_POSTFIX_GUI = "-ComputeMaster-";

   private static final Logger _log = Logger.getLogger(VcVlsi.class.getName());

   static final String VC_PROP_VM_NAME = "name";
   static final String VC_PROP_VM_EXTRA_CONFIG = "config.extraConfig";
   static final String VC_PROP_VM_UUID = "config.uuid";
   static final String VC_PROP_VM_NUM_CPU = "config.hardware.numCPU";
   static final String VC_PROP_VM_POWER_STATE = "runtime.powerState";
   static final String VC_PROP_VM_HOST = "runtime.host";
   static final String VC_PROP_VM_GUEST_NIC_INFO = "guest.net";
   static final String VC_PROP_VM_GUEST_HOSTNAME = "guest.hostName";

   static final String VC_MOREF_TYPE_TASK = "Task";
   static final String VC_MOREF_TYPE_VM = "VirtualMachine";
   static final String VC_MOREF_TYPE_FOLDER = "Folder";
   static final String VC_MOREF_TYPE_CONTAINER_VIEW = "ContainerView";
   private static final TypeNameImpl typeTask = new TypeNameImpl(VC_MOREF_TYPE_TASK);
   private static final TypeNameImpl typeVM = new TypeNameImpl(VC_MOREF_TYPE_VM);
   private static final TypeNameImpl typeFolder = new TypeNameImpl(VC_MOREF_TYPE_FOLDER);
   private static final TypeNameImpl typeContainerView = new TypeNameImpl(VC_MOREF_TYPE_CONTAINER_VIEW);

   private static final int propertyCollectorTimeout = ExternalizedParameters.get().getInt("VC_PROPERTY_COLLECTOR_TIMEOUT");

   static final String VHM_EXTRA_CONFIG_PREFIX = "vhmInfo.";
   static final String VHM_EXTRA_CONFIG_UUID = "vhmInfo.serengeti.uuid";
   static final String VHM_EXTRA_CONFIG_MASTER_UUID = "vhmInfo.masterVM.uuid";
   static final String VHM_EXTRA_CONFIG_MASTER_MOREF = "vhmInfo.masterVM.moid";
   static final String VHM_EXTRA_CONFIG_MASTER_CLUSTERNAME = "vhmInfo.masterVM.clusterName";
   static final String VHM_EXTRA_CONFIG_ELASTIC = "vhmInfo.elastic";
   static final String VHM_EXTRA_CONFIG_AUTOMATION_ENABLE = "vhmInfo.vhm.enable";
   static final String VHM_EXTRA_CONFIG_AUTOMATION_MIN_INSTANCES = "vhmInfo.min.computeNodeNum";
   static final String VHM_EXTRA_CONFIG_AUTOMATION_INSTANCE_RANGE = "vhmInfo.instanceRange.computeNodeNum";
   static final String VHM_EXTRA_CONFIG_JOB_TRACKER_PORT = "vhmInfo.jobtracker.port";

   private static final String TASK_INFO_STATE = "info.state";

   static final String WAIT_FOR_UPDATES_CANCELED_STATUS = "VC_WAIT_FOR_UPDATES_CANCELED";
   static final String WAIT_FOR_UPDATES_INVALID_COLLECTOR_VERSION_STATUS = "VC_WAIT_FOR_UPDATES_INVALID_COLLECTOR_VERSION";
   static final String WAIT_FOR_UPDATES_INVALID_PROPERTY_STATUS = "VC_WAIT_FOR_UPDATES_INVALID_PROPERTY";
   static final String WAIT_FOR_UPDATES_NO_CLUSTERS = "VC_WAIT_FOR_UPDATES_NO_CLUSTERS";

   private final String VC_ALARM_NAME_BASE = ExternalizedParameters.get().getString("VC_ALARM_NAME_BASE");

   private ThreadLocalCompoundStatus _threadLocalStatus;
   private PropertyCollector _blockedPropertyCollectorSingleton;     /* Only one thread will ever be blocked on the PropertyCollector */
   private Object _propertyCollectorLock = new Object();
   private Alarm _alarmSingleton;
   private Object _alarmLock = new Object();

   static {
      VmodlContext.initContext(new String[] { "com.vmware.vim.binding.vim" });
   }

   void setThreadLocalCompoundStatus(ThreadLocalCompoundStatus tlcs) {
      _threadLocalStatus = tlcs;
   }

   private CompoundStatus getCompoundStatus() {
      if (_threadLocalStatus == null) {
         return new CompoundStatus("DUMMY_STATUS");
      }
      return _threadLocalStatus.get();
   }

   private ThumbprintVerifier getThumbprintVerifier(final String vcThumbprint) {
      return new ThumbprintVerifier() {
         @Override
         public Result verify(String thumbprint) {
            if (vcThumbprint == null) {
               return Result.MATCH;
            } else if (thumbprint.equalsIgnoreCase(vcThumbprint)) {
               return Result.MATCH;
            } else {
               _log.log(Level.SEVERE, "VHM: thumbprint from vhm.properties does not match thumbprint from vCenter - "+thumbprint);
               return Result.MISMATCH;
            }
         }

         @Override
         public void onSuccess(X509Certificate[] chain, String thumbprint,
               Result verifyResult, boolean trustedChain,
               boolean verifiedAssertions) throws SSLException {
         }
      };
   }

   private ServiceInstance getServiceInstance(Client vcClient) {
      ManagedObjectReference svcRef = new ManagedObjectReference();
      svcRef.setType("ServiceInstance");
      svcRef.setValue("ServiceInstance");
      return vcClient.createStub(ServiceInstance.class, svcRef);
   }

   private ServiceInstanceContent getServiceInstanceContent(Client vcClient) throws ConnectionException {
      ServiceInstance svc = getServiceInstance(vcClient);
      return svc.retrieveContent();
   }

   /*
    * Create a temporary connection to VC to login using extension certificate via sdkTunnel,
    * and get the session ticket to use for the normal connection.
    */
   private String getSessionTicket(String vcIP, String keyStoreFile, String keyStorePwd, String vcExtKey, String vcThumbprint, long timeoutMillis)
         throws ConnectionException, URISyntaxException, IOException, GeneralSecurityException, VimFault {
      URI uri = new URI("https://sdkTunnel:8089/sdk/vimService");
      KeyStore keyStore = KeyStore.getInstance("JKS");
      InputStream is = null;
      try {
         is = new FileInputStream(keyStoreFile);
         keyStore.load(is, keyStorePwd.toCharArray());
      } finally {
         if (is != null) {
            try {
               is.close();
            } catch (IOException e) {}
         }
      }

      HttpConfigurationImpl httpConfig = new HttpConfigurationImpl();
      httpConfig.setKeyStore(keyStore);
      httpConfig.setDefaultProxy(vcIP, 80, "http");
      httpConfig.getKeyStoreConfig().setKeyAlias(keyStore.aliases().nextElement());
      httpConfig.getKeyStoreConfig().setKeyPassword(keyStorePwd);

      httpConfig.setTimeoutMs((int)timeoutMillis);
      httpConfig.setThumbprintVerifier(getThumbprintVerifier(vcThumbprint));

      HttpClientConfiguration clientConfig = HttpClientConfiguration.Factory.newInstance();
      clientConfig.setHttpConfiguration(httpConfig);
      Client newClient = Client.Factory.createClient(uri, version8.class, clientConfig);

      ServiceInstanceContent sic = getServiceInstanceContent(newClient);
      SessionManager sm = newClient.createStub(SessionManager.class, sic.getSessionManager());
      sm.loginExtensionByCertificate(vcExtKey, null);
      String ticket = sm.acquireSessionTicket(null);

      return ticket;
   }

   public Client connect(VcCredentials credentials, boolean useKey, Client cloneClient, long timeoutMillis)
         throws ConnectionException, HostConnectFault, IOException, VimFault, GeneralSecurityException, URISyntaxException {
      String sessionTicket = null;

      if (cloneClient != null) {
         ServiceInstanceContent sic = getServiceInstanceContent(cloneClient);
         SessionManager sm = cloneClient.createStub(SessionManager.class, sic.getSessionManager());
         sessionTicket = sm.acquireCloneTicket();
      } else if (useKey) {
         sessionTicket = getSessionTicket(credentials.vcIP, credentials.keyStoreFile, credentials.keyStorePwd, credentials.vcExtKey, credentials.vcThumbprint, timeoutMillis);
      }

      URI uri = new URI("https", null, credentials.vcIP, 443, "/sdk", null, null);

      // each VLSI call consumes an executor thread for the duration of the blocking call
      ThreadPoolExecutor executor =
            new ThreadPoolExecutor(1,  // core pool size
                  4,  // max pool size
                  10, TimeUnit.SECONDS, // max thread idle time
                  new LinkedBlockingQueue<Runnable>()); // work queue

      HttpConfigurationImpl httpConfig = new HttpConfigurationImpl();
      httpConfig.setTimeoutMs((int)timeoutMillis);
      httpConfig.setThumbprintVerifier(getThumbprintVerifier(credentials.vcThumbprint));

      HttpClientConfiguration clientConfig = HttpClientConfiguration.Factory.newInstance();
      clientConfig.setHttpConfiguration(httpConfig);
      clientConfig.setExecutor(executor);

      Client newClient = Client.Factory.createClient(uri, version8.class, clientConfig);

      if (newClient != null) {
         ServiceInstanceContent sic = getServiceInstanceContent(newClient);
         SessionManager sm = newClient.createStub(SessionManager.class, sic.getSessionManager());

         if (cloneClient != null) {
            sm.cloneSession(sessionTicket);
         } else {
            if (useKey) {
               sm.loginBySessionTicket(sessionTicket);
            } else {
               sm.login(credentials.user, credentials.password, null);
            }
         }
      }

      return newClient;
   }

   public boolean testConnection(Client vcClient) {
      if (vcClient == null) {
         return false;
      }
      // Test the operation of the current connection using the standard simple call for this purpose.
      Calendar vcTime = null;
      try {
         ServiceInstance si = getServiceInstance(vcClient);
         vcTime = si.currentTime();
      } catch (Exception e) {
         /* We don't really care what the exception was, we just don't want it propogating */
      } finally {
         if (vcTime == null) {
            _log.log(Level.INFO, "testConnection found VC connection dropped; caller will reconnect");
         }
      }
      return (vcTime != null);
   }


   private Folder getRootFolder(Client client) throws ConnectionException {
      ServiceInstanceContent sic = getServiceInstanceContent(client);
      return client.createStub(Folder.class, sic.getRootFolder());
   }

   /**
    * Property filters are used a lot when querying the JAX-WS API for information about VC entities
    * The code is pretty ugly, so it makes sense to encapsulate it in a utility class.
    * The class is dual-purpose - it can be created with either constructor depending on the need.
    * Properties can then be added to the filter and once that's completed,
    * retrieveProperties() or getPropertyCollector() can be called, depending on the requirement
    *
    */
   public class PropertyFilter {
      PropertyCollector _propertyCollector;
      Filter _filter;
      PropertySpec _propertySpec;
      FilterSpec _propertyFilterSpec;
      boolean _initialized = false;
      ContainerView _containerView;
      Client _vcClient;

      public PropertyFilter(Client vcClient, ContainerView containerView, TypeName type) {
         _vcClient = vcClient;
         _containerView = containerView;
         TraversalSpec tSpec = getTraversalSpecFromView();
         ObjectSpec objectSpec = new ObjectSpec();
         objectSpec.setObj(containerView._getRef());
         objectSpec.setSelectSet(new SelectionSpec[] {tSpec});
         _propertyFilterSpec = new FilterSpec();
         _propertyFilterSpec.setObjectSet(new ObjectSpec[] {objectSpec});
         _propertySpec = new PropertySpec();
         _propertySpec.setAll(Boolean.FALSE);
         _propertySpec.setType(type);
      }

      public PropertyFilter(Client vcClient, Task task) {
         _vcClient = vcClient;
         ObjectSpec objectSpec = new ObjectSpec();
         objectSpec.setObj(task._getRef());
         objectSpec.setSkip(Boolean.FALSE);
         _propertyFilterSpec = new FilterSpec();
         _propertyFilterSpec.setObjectSet(new ObjectSpec[] {objectSpec});
         _propertySpec = new PropertySpec();
         _propertySpec.setType(typeTask);
      }

      private TraversalSpec getTraversalSpecFromView() {
         // Create a traversal spec that starts from the ListView object
         // and traverses to its "view" property containing the managed object references.

         TraversalSpec viewToObject = new TraversalSpec();
         viewToObject.setName("viewToObject");
         viewToObject.setType(typeContainerView);
         viewToObject.setPath("view");
         viewToObject.setSkip(false);

         return viewToObject;
      }

      public void setPropToFilter(String property) throws InvalidProperty, ConnectionException {
         _propertySpec.setPathSet(new String[] {property});
         init();
      }

      public void setPropsToFilter(String[] properties) throws InvalidProperty, ConnectionException {
         _propertySpec.setPathSet(properties);
         init();
      }

      private void init() throws InvalidProperty, ConnectionException  {
         if (!_initialized) {
            _propertyFilterSpec.setPropSet(new PropertySpec [] {_propertySpec});

            ServiceInstanceContent sic = getServiceInstanceContent(_vcClient);
            _propertyCollector = _vcClient.createStub(PropertyCollector.class, sic.getPropertyCollector());

            _filter = _vcClient.createStub(Filter.class, _propertyCollector.createFilter(_propertyFilterSpec, true));
         }
         _initialized = true;
      }

      public PropertyCollector getPropertyCollector() throws InvalidProperty, ConnectionException  {
         init();
         return _propertyCollector;
      }

      public RetrieveResult retrieveProperties() throws InvalidProperty, ConnectionException {
         init();
         RetrieveOptions retrieveOptions = new RetrieveOptions();

         return _propertyCollector.retrievePropertiesEx(new FilterSpec[] {_propertyFilterSpec}, retrieveOptions);
      }

      public RetrieveResult continueRetrieve(String token) throws InvalidProperty {
         return _propertyCollector.continueRetrievePropertiesEx(token);
      }

      public void cleanup() {
         if (_filter != null) {
            _filter.destroy();
         }
         if (_containerView != null) {
            _containerView.destroy();
         }
      }

   }

   private List<ManagedObjectReference> findObjectsInFolder(Client client, Folder baseFolder, TypeName type, String restrictToName)
         throws InvalidProperty, ConnectionException {
      if (baseFolder == null) {
         return null;
      }

      List<ManagedObjectReference> resultRefs = new ArrayList<ManagedObjectReference>();
      ServiceInstanceContent sic = getServiceInstanceContent(client);

      ViewManager viewMgr = client.createStub(ViewManager.class, sic.getViewManager());
      ContainerView cView = client.createStub(ContainerView.class,
            viewMgr.createContainerView(baseFolder._getRef(), new TypeName[] {type}, true));

      PropertyFilter propFilter = new PropertyFilter(client, cView, type);
      propFilter.setPropToFilter("name");

      RetrieveResult rr = propFilter.retrieveProperties();
      boolean done = false;

      while ((rr != null) && !done) {
         ObjectContent[] oca = rr.getObjects();

         for (ObjectContent oc : oca) {
            if (restrictToName == null) {
               resultRefs.add(oc.getObj());
            } else {
               // filter out by name
               DynamicProperty[] dps = oc.getPropSet();
               for (DynamicProperty dp : dps) {
                  if (dp.getName().equals("name") && dp.getVal().equals(restrictToName)) {
                     resultRefs.add(oc.getObj());
                     done = true;
                     break;
                  }
               }
            }
         }
         if (rr.getToken() == null) {
            done = true;
         } else if (!done) {
            // get the next batch of results from VC
            rr = propFilter.continueRetrieve(rr.getToken());
         }
      }

      propFilter.cleanup();
      return resultRefs;
   }

   Folder getFolderForName(Client client, Folder baseFolder, String restrictToName) throws InvalidProperty {
      if (baseFolder == null) {
         baseFolder = getRootFolder(client);
      }
      List<ManagedObjectReference> refs = findObjectsInFolder(client, baseFolder, typeFolder, restrictToName);
      if (refs.size() > 0) {
         return client.createStub(Folder.class, refs.get(0));
      }
      return null;
   }

   private PropertyFilter setupWaitForUpdates(Client vcClient, Folder baseFolder, TypeName type, String[] statePropsToGet)
         throws InvalidProperty, ConnectionException {
      PropertyFilter propFilter = null;
      ServiceInstanceContent sic = getServiceInstanceContent(vcClient);

      ViewManager viewMgr = vcClient.createStub(ViewManager.class, sic.getViewManager());
      ContainerView cView = vcClient.createStub(ContainerView.class,
            viewMgr.createContainerView(baseFolder._getRef(), new TypeName[] {type}, true));

      propFilter = new PropertyFilter(vcClient, cView, type);

      propFilter.setPropsToFilter(statePropsToGet);
      return propFilter;
   }

   private UpdateSet callWaitForUpdates(PropertyCollector propCollector, String version) throws InvalidCollectorVersion {
      UpdateSet updateSet = null;
      if (version == null) {
         version = "";
      }

      WaitOptions waitOptions = new WaitOptions();
      waitOptions.setMaxWaitSeconds(propertyCollectorTimeout);

      synchronized(_propertyCollectorLock) {
         _blockedPropertyCollectorSingleton = propCollector;
      }
      try {
         updateSet = propCollector.waitForUpdatesEx(version, waitOptions);
      } finally {
         synchronized(_propertyCollectorLock) {
            _blockedPropertyCollectorSingleton = null;
         }
      }
      return updateSet;
   }

   /* TODO: Why is this not called anywhere? */
   private void cleanupWaitForUpdates(PropertyFilter propFilter) {
      propFilter.cleanup();
   }

   private static Map<String, Set<String>> getNicInfo(NicInfo[] nicInfoArray) {
      Map<String, Set<String>> nicAndIpAddressMap = new HashMap<String, Set<String>>();
      if (nicInfoArray != null) {
         for (NicInfo nicInfo : nicInfoArray) {
            String networkName = nicInfo.getNetwork();
            IpConfigInfo ipConfigInfo = nicInfo.getIpConfig();
            if ((ipConfigInfo != null) && (networkName != null)) {
               IpAddress[] ipAddressObjects = ipConfigInfo.getIpAddress();
               if (ipAddressObjects != null) {
                  Set<String> ipAddressSet = new HashSet<String>();
                  for (IpAddress ipAddressObj : ipAddressObjects) {
                     String ipAddress = ipAddressObj.getIpAddress();
                     if (ipAddress != null) {
                        ipAddressSet.add(ipAddress);
                     }
                  }
                  nicAndIpAddressMap.put(networkName, ipAddressSet);
               }
            }
         }
      }
      return nicAndIpAddressMap;
   }

   private static VMEventData parseObjUpdate(Logger logger, ObjectUpdate obj) {
      VMEventData vmData = new VMEventData();
      vmData._vmMoRef = obj.getObj().getValue();

      Kind kind = obj.getKind();
      logger.log(Level.FINE, "Pobj kind= " + kind + " obj= " + obj.getObj().getValue());
      if (kind == Kind.leave) {
         vmData._isLeaving = true;
      } else if (kind == Kind.modify || kind == Kind.enter) {
         vmData._isLeaving = false;
         for (Change pc : obj.getChangeSet()) {
            String pcName = pc.getName();
            Object pcValue = pc.getVal();
            logger.log(Level.FINE, "Pobj prop= " + pcName + " val= " + pcValue);
            if (pcValue != null) {
               if (pcName.equals(VC_PROP_VM_UUID)) {
                  vmData._myUUID = (String)pcValue;
               } else if (pcName.equals(VC_PROP_VM_NUM_CPU)) {
                  vmData._vCPUs = (Integer)pcValue;
               } else if (pcName.equals(VC_PROP_VM_NAME)) {
                  vmData._myName = (String)pcValue;
                  /* Update this as early as possible. Doesn't matter if the key already exists */
                  logger.log(Level.FINE, "Associating vmId "+vmData._vmMoRef+" with name "+vmData._myName);
                  LogFormatter._vmIdToNameMapper.put(vmData._vmMoRef, vmData._myName);
               } else if (pcName.equals(VC_PROP_VM_POWER_STATE)) {
                  PowerState ps = (PowerState)pcValue;
                  if (ps == PowerState.poweredOn) {
                     vmData._powerState = true;
                  } else {
                     vmData._powerState = false;
                  }
               } else if (pcName.equals(VC_PROP_VM_HOST)) {
                  vmData._hostMoRef = ((ManagedObjectReference)pcValue).getValue();
               } else if (pcName.equals(VC_PROP_VM_GUEST_NIC_INFO)) {
                  vmData._nicAndIpAddressMap = getNicInfo((NicInfo[])pcValue);
               } else if (pcName.equals(VC_PROP_VM_GUEST_HOSTNAME)) {
                  vmData._dnsName = (String)pcValue;
               } else if (pcName.equals(VC_PROP_VM_EXTRA_CONFIG)) {
                  // extraConfig updates can be returned as an array (pcName == config.extraConfig), or individual key (below)
                  OptionValue[] ecl = (OptionValue[]) pcValue;
                  for (OptionValue ec : ecl) {
                     if (ec.getKey().startsWith(VHM_EXTRA_CONFIG_PREFIX)) {
                        VcVlsiHelper.parseExtraConfig(vmData, ec.getKey(), (String)ec.getValue());
                     }
                  }
               } else if (pcName.lastIndexOf(VC_PROP_VM_EXTRA_CONFIG) >= 0) {
                  // individual extraConfig entries (pcName = config.extraConfig["xxx"].value)
                  String [] parts = pcName.split("\"",3);
                  if (parts.length > 1) {
                     if (parts[1].startsWith(VHM_EXTRA_CONFIG_PREFIX)) {
                        // sometimes pcValue is a String, and sometimes its OptionValue...
                        String valueString;
                        if (pcValue instanceof String) {
                           valueString = (String)pcValue;
                        } else {
                           valueString = (String) ((OptionValue)pcValue).getValue();
                        }
                        VcVlsiHelper.parseExtraConfig(vmData, parts[1], valueString);
                     }
                  }
               } else {
                  logger.log(Level.WARNING, "Unexpected update: prop= " + pcName + " val= " + pcValue);
               }
            }
         }
      }
      return vmData;
   }


   private String pcVMsInFolder(Client vcClient, Folder folder, String version, List<VMEventData> vmDataList)
         throws ConnectionException, InvalidCollectorVersion, InvalidProperty {
      if (version == null) {
         version = "";
      }
      if (version.equals("")) {
         String [] props = {VC_PROP_VM_NAME, VC_PROP_VM_EXTRA_CONFIG, VC_PROP_VM_UUID, VC_PROP_VM_NUM_CPU,
               VC_PROP_VM_POWER_STATE, VC_PROP_VM_HOST, VC_PROP_VM_GUEST_NIC_INFO, VC_PROP_VM_GUEST_HOSTNAME};
         setupWaitForUpdates(vcClient, folder, typeVM, props);
      }
      ServiceInstanceContent sic = getServiceInstanceContent(vcClient);
      PropertyCollector propertyCollector = vcClient.createStub(PropertyCollector.class, sic.getPropertyCollector());

      UpdateSet updateSet = null;
      try {
         updateSet = callWaitForUpdates(propertyCollector, version);
      } catch (ConnectionException e) {
         Throwable cause = e.getCause();
         /*
          * SocketTimeoutException is caused when we hit SESSION_TIME_OUT
          * If that happens, hide the exception, and just return with no changes
          */
         if ((cause != null) && (cause instanceof SocketTimeoutException)) {
            return version;
         }
         throw e;
      }

      if (updateSet != null) {
         version = updateSet.getVersion();
         FilterUpdate[] updates = updateSet.getFilterSet();

         //_log.log(Level.INFO, "WFU new version= " + version + " fs= " + updates);
         if (updates != null) {
            for (FilterUpdate pfu : updates) {
               ObjectUpdate[] objectSet = pfu.getObjectSet();

               for (ObjectUpdate obj : objectSet) {
                  VMEventData vmData = parseObjUpdate(_log, obj);
                  if (vmData != null) {
                     vmDataList.add(vmData);
                  }
               }
            }
         }
      }
      return version;
   }

   boolean waitForTask(Client client, Task task) {
      CompoundStatus status = new CompoundStatus("waitForTask");
      boolean result = false;
      PropertyFilter propFilter = new PropertyFilter(client, task);
      try {
         propFilter.setPropsToFilter(new String [] {TASK_INFO_STATE });
         UpdateSet updateSet = null;
         String version = "";

         WaitOptions waitOptions = new WaitOptions();
         waitOptions.setMaxWaitSeconds(propertyCollectorTimeout);

         _mainLoop: while (true) {

            _log.log(Level.INFO, "WFT waiting vesrion=" + version);
            updateSet = propFilter.getPropertyCollector().waitForUpdatesEx(version, waitOptions);
            if (updateSet != null) {
               version = updateSet.getVersion();
               FilterUpdate[] updates = updateSet.getFilterSet();

               if (updates != null) {
                  for (FilterUpdate pfu : updates) {
                     ObjectUpdate[] objectSet = pfu.getObjectSet();

                     for (ObjectUpdate obj : objectSet) {
                        Kind kind = obj.getKind();
                        if (kind == Kind.modify || kind == Kind.enter || kind == Kind.leave) {
                           for (Change pc : obj.getChangeSet()) {
                              String pcName = pc.getName();
                              Object pcValue = pc.getVal();
                              if (pcName.lastIndexOf(TASK_INFO_STATE) >= 0) {
                                 TaskInfo.State state = (TaskInfo.State)pcValue;
                                 if (state == TaskInfo.State.error) {
                                    result = false;
                                    break _mainLoop;
                                 } else if (state == TaskInfo.State.success) {
                                    result = true;
                                    break _mainLoop;
                                 }
                              }
                           }
                        }
                     }
                  }
               }
            }
         }
         status.registerTaskSucceeded();
      } catch (ConnectionException e) {
         reportException("Error connecting to vCenter: "+e.getMessage(), status);
      } catch (Exception e) {
         reportException("Unexpected exception waiting for task completion", e, status);
      } finally {
         propFilter.cleanup();
      }
      getCompoundStatus().addStatus(status);
      return result;
   }

   public String waitForUpdates(Client client, String baseFolderName, String version, List<VMEventData> vmDataList) {
      CompoundStatus status = new CompoundStatus("waitForUpdates");
      String newVersion = version;
      /* There is an expectation that this method should never return null */
      if (newVersion == null) {
         newVersion = "";
      }
      try {
         Folder f = getFolderForName(client, null, baseFolderName);
         if (f == null) {
            // This is normal state when user hasn't created any hadoop clusters yet
            _log.log(Level.INFO, "No found clusters for hadoop UUID " + baseFolderName);
            newVersion = WAIT_FOR_UPDATES_NO_CLUSTERS;
         } else {
            newVersion = pcVMsInFolder(client, f, version, vmDataList);
         }
         status.registerTaskSucceeded();
      } catch (RequestCanceled e) {
         _log.info("waitForUpdates request has been canceled");
         newVersion = WAIT_FOR_UPDATES_CANCELED_STATUS;
      } catch (InvalidCollectorVersion e) {
         _log.info("propertyCollector version has become stale");
         newVersion = WAIT_FOR_UPDATES_INVALID_COLLECTOR_VERSION_STATUS;
      } catch (InvalidProperty e) {
         _log.info("propertyCollector property is invalid: "+e);
         newVersion = WAIT_FOR_UPDATES_INVALID_PROPERTY_STATUS;
      } catch (ConnectionException e) {
         reportException("Error connecting to vCenter: "+e.getMessage(), status);
      } catch (TransportProtocolException e) {
         reportException("Error connecting to vCenter: "+e.getMessage(), status);
      } catch (Exception e) {
         reportException("Unexpected exception waiting for updates", e, status);
      }
      getCompoundStatus().addStatus(status);
      return newVersion;
   }

   public List<String> getVMsInFolder(Client client, String baseFolderName, String folderName) {
      CompoundStatus status = new CompoundStatus("getVMsInFolder");
      List<String> result = null;
      try {
         Folder baseFolder = getFolderForName(client, null, baseFolderName);

         Folder folder = getFolderForName(client, baseFolder, folderName);

         List<ManagedObjectReference> refs = findObjectsInFolder(client, folder, typeVM, null);
         if ((refs != null) && (refs.size() > 0)) {
            result = new ArrayList<String>();
            for (ManagedObjectReference ref : refs) {
               result.add(ref.getValue());
            }
         }
         status.registerTaskSucceeded();
      } catch (com.vmware.vim.binding.vmodl.fault.RequestCanceled e) {
         _log.info("getVMsInFolder has been canceled");
      } catch (ConnectionException e) {
         reportException("Error connecting to vCenter: "+e.getMessage(), status);
      } catch (Exception e) {
         reportException("Unexpected exception in getVMsInFolder", e, status);
      }
      getCompoundStatus().addStatus(status);
      return result;
   }

   private void reportException(String msg, CompoundStatus status) {
      reportException(msg, null, status);
   }

   private void reportException(String msg, Exception e, CompoundStatus status) {
      if (e != null) {
         /* Stack trace at INFO level to comply with PSP */
         _log.log(Level.INFO, msg, e);
         msg += ": "+e.getMessage();
      }
      _log.log(Level.WARNING, msg);
      status.registerTaskFailed(false, msg);
   }

   public Map<String, Task> powerOnVMs(Client client, Set<String> vmMoRefs) {
      CompoundStatus status = new CompoundStatus(VCActions.VC_POWER_ON_STATUS_KEY);
      Map<String, Task> result = new HashMap<String, Task>();
      for (String moRef : vmMoRefs) {
         ManagedObjectReference ref = new ManagedObjectReference();
         ref.setValue(moRef);
         VirtualMachine vm = client.createStub(VirtualMachine.class, ref);
         try {
            ManagedObjectReference taskRef = vm.powerOn(null);
            Task task = client.createStub(Task.class, taskRef);
            result.put(moRef, task);
            status.registerTaskSucceeded();
         } catch (Exception e) {
            reportException("Error powering on VM: "+e.getMessage(), status);
         }
      }
      getCompoundStatus().addStatus(status);
      return result;
   }

   public Map<String, Task> powerOffVMs(Client client, Set<String> vmMoRefs) {
      CompoundStatus status = new CompoundStatus(VCActions.VC_POWER_OFF_STATUS_KEY);
      Map<String, Task> result = new HashMap<String, Task>();
      for (String moRef : vmMoRefs) {
         ManagedObjectReference ref = new ManagedObjectReference();
         ref.setValue(moRef);
         VirtualMachine vm = client.createStub(VirtualMachine.class, ref);
         try {
            ManagedObjectReference taskRef = vm.powerOff();
            Task task = client.createStub(Task.class, taskRef);
            result.put(moRef, task);
            status.registerTaskSucceeded();
         } catch (Exception e) {
            reportException("Error powering off VM: "+e.getMessage(), status);
         }
      }
      getCompoundStatus().addStatus(status);
      return result;
   }

   public void cancelWaitForUpdates() {
      synchronized(_propertyCollectorLock) {
         if (_blockedPropertyCollectorSingleton != null) {
            _blockedPropertyCollectorSingleton.cancelWaitForUpdates();
         }
      }
   }

   public PerformanceManager getPerformanceManager(Client client) {
      try {
         ServiceInstanceContent sic = getServiceInstanceContent(client);
         return client.createStub(PerformanceManager.class, sic.getPerfManager());
      } catch (Exception e) {
         _log.info("Cannot connect to VC performance manager");
         return null;
      }
   }

   private EventManager getEventManager(Client client) {
      try {
         ServiceInstanceContent sic = getServiceInstanceContent(client);
         return client.createStub(EventManager.class, sic.getEventManager());
      } catch (Exception e) {
         _log.info("Cannot connect to VC event manager");
         return null;
      }
   }

   private AlarmManager getAlarmManager(Client client) {
      try {
         ServiceInstanceContent sic = getServiceInstanceContent(client);
         return client.createStub(AlarmManager.class, sic.getAlarmManager());
      } catch (Exception e) {
         _log.info("Cannot connect to VC alarm manager");
         return null;
      }
   }

   public boolean postEventForVM(Client client, String vmMoRef, EventSeverity level, String message) {
      EventManager eventManager = getEventManager(client);
      if (eventManager == null) {
         return false;
      }

      ManagedObjectReference ref = new ManagedObjectReference();
      ref.setValue(vmMoRef);
      ref.setType("VirtualMachine");

      EventEx event = new EventExImpl();
      event.setCreatedTime(Calendar.getInstance());
      event.setUserName("Big Data Extensions");
      event.setEventTypeId("com.vmware.vhadoop.vhm.vc.events."+level.name());
      event.setSeverity(level.name());
      event.setMessage(message);
      event.setObjectId(ref.getValue());
      event.setObjectType(new TypeNameImpl("VirtualMachine"));

      try {
         _log.log(VhmLevel.USER, "VHM: <%V"+vmMoRef+"%V> - "+message);
         eventManager.postEvent(event, null);
         return true;
      } catch (InvalidEvent e) {
         _log.log(Level.INFO, "VHM: <%V"+vmMoRef+"%V> - failed to log "+level.name()+" event with vCenter", e);
      }
      return false;
   }


   private Alarm getAlarm(Client client, String rootFolderName) {
      synchronized(_alarmLock) {
         if (_alarmSingleton != null) {
            return _alarmSingleton;
         }
         AlarmManager manager = getAlarmManager(client);
         if (manager == null) {
            return null;
         }

         Folder root;
         try {
            root = getFolderForName(client, null, rootFolderName);

            ManagedObjectReference[] existing = manager.getAlarm(root._getRef());
            for (ManagedObjectReference m : existing) {
               Alarm a = client.createStub(Alarm.class, m);
               if (a.getInfo().getName().startsWith(VC_ALARM_NAME_BASE)) {
                  _alarmSingleton = a;
                  return _alarmSingleton;
               }
            }
         } catch (InvalidProperty e) {
            _log.info("VHM: unable to get reference to alarm "+VC_ALARM_NAME_BASE+" on vApp folder "+rootFolderName);
            _log.log(Level.FINER, "VHM: exception while getting reference to top level alarm", e);
         } catch (NullPointerException e) {
            /* almost any of the values returned from vlsi client or their subsequent calls could be null but
             * will not be most of the time. It's much clearer to just have one catch here than tests on
             * ever access given we do the same thing in response.
             */
            _log.info("VHM: unable to get reference to alarm "+VC_ALARM_NAME_BASE+" on vApp folder "+rootFolderName);
            _log.log(Level.FINER, "VHM: exception while getting reference to top level alarm", e);
         }
      }

      return null;
   }

   void acknowledgeAlarm(Client client, String rootFolderName, String vmMoRef) {
      AlarmManager alarmMgr = getAlarmManager(client);
      if (alarmMgr == null) {
         return;
      }

      /* acknowledge the alarm */
      Alarm alarm = getAlarm(client, rootFolderName);
      if (alarm != null) {
         ManagedObjectReference moRef = new ManagedObjectReference();
         moRef.setValue(vmMoRef);
         moRef.setType("VirtualMachine");

         alarmMgr.acknowledgeAlarm(alarm._getRef(), moRef);
      }
   }

}
   /*

   VirtualMachine vm = getVMForName(f, "xxxxx");
   _log.log(Level.INFO, "TPC vm.name = " + vm.getName());
   _log.log(Level.INFO, "TPC vm = " + vm);
   PowerState ps = vm.getRuntime().getPowerState();
   _log.log(Level.INFO, "TPC vm ps = " + ps);
   ManagedObjectReference taskRef;
   if (ps == PowerState.poweredOn) {
      taskRef = vm.powerOff();
   } else {
       taskRef = vm.powerOn(null);
   }
   Task t = defaultClient.createStub(Task.class, taskRef);
   boolean success = waitForTask(t);
   _log.log(Level.INFO, "TPC task success=" + success);

   ps = vm.getRuntime().getPowerState();
   _log.log(Level.INFO, "TPC vm new ps = " + ps);
*/

