package com.vmware.vhadoop.vhm.vc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLException;

import com.vmware.vim.binding.impl.vmodl.TypeNameImpl;
import com.vmware.vim.binding.vim.Folder;
import com.vmware.vim.binding.vim.ServiceInstance;
import com.vmware.vim.binding.vim.ServiceInstanceContent;
import com.vmware.vim.binding.vim.SessionManager;
import com.vmware.vim.binding.vim.Task;
import com.vmware.vim.binding.vim.TaskInfo;
import com.vmware.vim.binding.vim.UserSession;
import com.vmware.vim.binding.vim.VirtualMachine;
import com.vmware.vim.binding.vim.VirtualMachine.PowerState;
import com.vmware.vim.binding.vim.fault.InvalidLocale;
import com.vmware.vim.binding.vim.fault.InvalidLogin;
import com.vmware.vim.binding.vim.fault.InvalidTicket;
import com.vmware.vim.binding.vim.fault.NoClientCertificate;
import com.vmware.vim.binding.vim.fault.NoHost;
import com.vmware.vim.binding.vim.fault.NotFound;
import com.vmware.vim.binding.vim.fault.NotSupportedHost;
import com.vmware.vim.binding.vim.fault.TooManyTickets;
import com.vmware.vim.binding.vim.fault.UserNotFound;
import com.vmware.vim.binding.vim.option.OptionValue;
import com.vmware.vim.binding.vim.version.version8;
import com.vmware.vim.binding.vim.view.ContainerView;
import com.vmware.vim.binding.vim.view.ViewManager;
import com.vmware.vim.binding.vmodl.DynamicProperty;
import com.vmware.vim.binding.vmodl.ManagedObjectReference;
import com.vmware.vim.binding.vmodl.TypeName;
import com.vmware.vim.binding.vmodl.fault.NotSupported;
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
import com.vmware.vim.vmomi.client.http.HttpClientConfiguration;
import com.vmware.vim.vmomi.client.http.ThumbprintVerifier;
import com.vmware.vim.vmomi.client.http.impl.HttpConfigurationImpl;
import com.vmware.vim.vmomi.core.types.VmodlContext;

public class VcVlsi {

   private static final int SESSION_TIME_OUT = 120000;
   
   private static final Logger _log = Logger.getLogger("VcVlsi");
   private static final VmodlContext vmodlContext = VmodlContext.initContext(new String[] { "com.vmware.vim.binding.vim" });
   private Client defaultClient;
   private String vcThumbprint = null;

   private static final String VC_MOREF_TYPE_TASK = "Task";
   private static final String VC_MOREF_TYPE_VM = "VirtualMachine";
   private static final String VC_MOREF_TYPE_FOLDER = "Folder";
   private static final String VC_MOREF_TYPE_CONTAINER_VIEW = "ContainerView";
   private static final TypeNameImpl typeTask = new TypeNameImpl(VC_MOREF_TYPE_TASK);
   private static final TypeNameImpl typeVM = new TypeNameImpl(VC_MOREF_TYPE_VM);
   private static final TypeNameImpl typeFolder = new TypeNameImpl(VC_MOREF_TYPE_FOLDER);
   private static final TypeNameImpl typeContainerView = new TypeNameImpl(VC_MOREF_TYPE_CONTAINER_VIEW);

   private static final int propertyCollectorTimeout = 300;

   private static final String VHM_EXTRA_CONFIG_PREFIX = "vhmInfo.";
   private static final String VHM_EXTRA_CONFIG_UUID = "vhmInfo.serengeti.uuid";
   private static final String VHM_EXTRA_CONFIG_MASTER_UUID = "vhmInfo.masterVM.uuid";
   private static final String VHM_EXTRA_CONFIG_MASTER_MOREF = "vhmInfo.masterVM.moid";
   private static final String VHM_EXTRA_CONFIG_ELASTIC = "vhmInfo.elastic";
   private static final String VHM_EXTRA_CONFIG_AUTOMATION_ENABLE = "vhmInfo.vhm.enable";
   private static final String VHM_EXTRA_CONFIG_AUTOMATION_MIN_INSTANCES = "vhmInfo.min.computeNodesNum";

   private static final String TASK_INFO_STATE = "info.state";

   private ThumbprintVerifier getThumbprintVerifier() {
      return new ThumbprintVerifier() {
         @Override
         public Result verify(String thumbprint) {
            if (vcThumbprint == null) {
               return Result.MATCH;
            } else if (thumbprint.equalsIgnoreCase(vcThumbprint)) {
               return Result.MATCH;
            } else {
               _log.log(Level.SEVERE, "Thumbprint mismatch: remote=" + thumbprint);
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

   public ServiceInstanceContent getServiceInstanceContent(Client vcClient) {

      ManagedObjectReference svcRef = new ManagedObjectReference();
      svcRef.setType("ServiceInstance");
      svcRef.setValue("ServiceInstance");
      ServiceInstance svc = vcClient.createStub(ServiceInstance.class, svcRef);
      
      return svc.retrieveContent();
   }
   
   /*
    * Create a temporary connection to VC to login using extension certificate via sdkTunnel,
    * and get the session ticket to use for the normal connection.
    */
   public String getSessionTicket(String vcIP, String keyStoreFile, String keyStorePwd, String vcExtKey)
         throws URISyntaxException, KeyStoreException, NoSuchAlgorithmException, CertificateException, FileNotFoundException, IOException, InvalidLogin, InvalidLocale, NoClientCertificate, NoHost, NotSupportedHost, NotFound, TooManyTickets {

      URI uri = new URI("https://sdkTunnel:8089/sdk/vimService"); 
      KeyStore keyStore = KeyStore.getInstance("JKS");
      keyStore.load(new FileInputStream(keyStoreFile), keyStorePwd.toCharArray());

      HttpConfigurationImpl httpConfig = new HttpConfigurationImpl();
      httpConfig.setKeyStore(keyStore);
      httpConfig.setDefaultProxy(vcIP, 80, "http");
      httpConfig.getKeyStoreConfig().setKeyAlias(keyStore.aliases().nextElement());
      httpConfig.getKeyStoreConfig().setKeyPassword(keyStorePwd);
      
      httpConfig.setTimeoutMs(SESSION_TIME_OUT);
      httpConfig.setThumbprintVerifier(getThumbprintVerifier());

      HttpClientConfiguration clientConfig = HttpClientConfiguration.Factory.newInstance();
      clientConfig.setHttpConfiguration(httpConfig);
      Client newClient = Client.Factory.createClient(uri, version8.class, clientConfig);
      
      ServiceInstanceContent sic = getServiceInstanceContent(newClient);
      SessionManager sm = newClient.createStub(SessionManager.class, sic.getSessionManager());
      UserSession us = sm.loginExtensionByCertificate(vcExtKey, null);
      String ticket = sm.acquireSessionTicket(null);
      
      return ticket;
   }

   private String getCloneTicket() {
      ServiceInstanceContent sic = getServiceInstanceContent(defaultClient);
      SessionManager sm = defaultClient.createStub(SessionManager.class, sic.getSessionManager());
      return sm.acquireCloneTicket();
   }

   
   public Client connect(VcCredentials credentials, boolean useKey, boolean cloneSession) 
         throws Exception {
      vcThumbprint = credentials.vcThumbprint;
      String sessionTicket = null;
      
      if (cloneSession) {
         sessionTicket = getCloneTicket();
      } else if (useKey) {
         sessionTicket = getSessionTicket(credentials.vcIP, credentials.keyStoreFile, credentials.keyStorePwd, credentials.vcExtKey);
      }
      
      URI uri = new URI("https://"+credentials.vcIP+":443/sdk");

      // each VLSI call consumes an executor thread for the duration of the blocking call 
      ThreadPoolExecutor executor =
            new ThreadPoolExecutor(1,  // core pool size
                  4,  // max pool size
                  10, TimeUnit.SECONDS, // max thread idle time
                  new LinkedBlockingQueue<Runnable>()); // work queue

      HttpConfigurationImpl httpConfig = new HttpConfigurationImpl();
      httpConfig.setTimeoutMs(SESSION_TIME_OUT);
      httpConfig.setThumbprintVerifier(getThumbprintVerifier());

      HttpClientConfiguration clientConfig = HttpClientConfiguration.Factory.newInstance();
      clientConfig.setHttpConfiguration(httpConfig);
      clientConfig.setExecutor(executor);

      Client newClient = Client.Factory.createClient(uri, version8.class, clientConfig);

      ServiceInstanceContent sic = getServiceInstanceContent(newClient);
      SessionManager sm = newClient.createStub(SessionManager.class, sic.getSessionManager());
      UserSession us = null;

      if (cloneSession) {
         us = sm.cloneSession(sessionTicket);
         _log.log(Level.INFO, "WFU us = " + us);
      } else {
         // set this as the default client
         defaultClient = newClient;
         if (useKey) {
            us = sm.loginBySessionTicket(sessionTicket);
         } else {
            us = sm.login(credentials.user, credentials.password, null);
         }
      }
      
      return newClient;
   }

   private Folder getRootFolder() {
      ServiceInstanceContent sic = getServiceInstanceContent(defaultClient);
      return defaultClient.createStub(Folder.class, sic.getRootFolder());
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

      /* TODO: Could this be called post-init? */
      public void setPropToFilter(String property) {
         _propertySpec.setPathSet(new String[] {property});
      }

      public void setPropsToFilter(String[] properties) {
         _propertySpec.setPathSet(properties);
      }

      private void init()  {
         if (!_initialized) {
            _propertyFilterSpec.setPropSet(new PropertySpec [] {_propertySpec});
            ServiceInstanceContent sic = getServiceInstanceContent(_vcClient);
            _propertyCollector = _vcClient.createStub(PropertyCollector.class, sic.getPropertyCollector());
         }
         _initialized = true;
      }

      public PropertyCollector getPropertyCollector() throws InvalidProperty {
         init();
         if (_filter == null) {
            _filter = _vcClient.createStub(Filter.class, _propertyCollector.createFilter(_propertyFilterSpec, true));
         }
         return _propertyCollector;
      }

      public ObjectContent[] retrieveProperties() throws InvalidProperty {
         init();
         RetrieveOptions retrieveOptions = new RetrieveOptions();
         RetrieveResult rr = _propertyCollector.retrievePropertiesEx(new FilterSpec[] {_propertyFilterSpec}, retrieveOptions);
         return rr.getObjects();
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

   private ArrayList<ManagedObjectReference> findObjectsInFolder(Folder baseFolder, TypeName type, String restrictToName) 
         throws InvalidProperty {
      ArrayList<ManagedObjectReference> resultRefs = new ArrayList<ManagedObjectReference>();
      ServiceInstanceContent sic = getServiceInstanceContent(defaultClient);
      
      ViewManager viewMgr = defaultClient.createStub(ViewManager.class, sic.getViewManager());
      ContainerView cView = defaultClient.createStub(ContainerView.class,
            viewMgr.createContainerView(baseFolder._getRef(), new TypeName[] {type}, true));

      PropertyFilter propFilter = new PropertyFilter(defaultClient, cView, type);
      propFilter.setPropToFilter("name");

      ObjectContent[] oca = propFilter.retrieveProperties();

      for (ObjectContent oc : oca) {
         if (restrictToName == null) {
            resultRefs.add(oc.getObj());
         } else {
            // filter out by name
            DynamicProperty[] dps = oc.getPropSet();
            for (DynamicProperty dp : dps) {
               if (dp.getName().equals("name") && dp.getVal().equals(restrictToName)) {
                  resultRefs.add(oc.getObj());
               }
            }
         }
      }
      propFilter.cleanup();
      return resultRefs;
   }

   private Folder getFolderForName(String restrictToName) throws InvalidProperty {
      Folder rootFolder = getRootFolder();
      ArrayList<ManagedObjectReference> refs = findObjectsInFolder(rootFolder, typeFolder, restrictToName);
      if (refs.size() > 0) {
         return defaultClient.createStub(Folder.class, refs.get(0));
      }
      return null;
   }

   private VirtualMachine getVMForName(Folder baseFolder, String restrictToName) throws InvalidProperty {
      ArrayList<ManagedObjectReference> refs = findObjectsInFolder(baseFolder, typeVM, restrictToName);
      if (refs.size() > 0) {
         return defaultClient.createStub(VirtualMachine.class, refs.get(0));
      }
      return null;
   }

   private ArrayList<VirtualMachine> listVMsinFolder(Folder baseFolder) throws InvalidProperty {
      ArrayList<ManagedObjectReference> refs = findObjectsInFolder(baseFolder, typeVM, null);
      ArrayList<VirtualMachine> vms = new ArrayList<VirtualMachine>();
      for (ManagedObjectReference ref : refs) {
         vms.add(defaultClient.createStub(VirtualMachine.class, ref));
      }
      return vms;
   }

   private PropertyFilter setupWaitForUpdates(Client vcClient, Folder baseFolder, TypeName type, String[] statePropsToGet) {
      PropertyFilter propFilter = null;
      try {
         ServiceInstanceContent sic = getServiceInstanceContent(vcClient);
         
         ViewManager viewMgr = vcClient.createStub(ViewManager.class, sic.getViewManager());
         ContainerView cView = vcClient.createStub(ContainerView.class,
               viewMgr.createContainerView(baseFolder._getRef(), new TypeName[] {type}, true));

         propFilter = new PropertyFilter(vcClient, cView, type);

         propFilter.setPropsToFilter(statePropsToGet);
      } catch (Exception e) {
         _log.log(Level.INFO, "Unexpected exception waiting for VC property change" + e);
         return null;
      }

      return propFilter;
   }
   
   private UpdateSet callWaitForUpdates(PropertyFilter propFilter, String version) {
      UpdateSet updateSet = null;
      try {
         if (version == null) {
            version = "";
         }

         WaitOptions waitOptions = new WaitOptions();
         waitOptions.setMaxWaitSeconds(propertyCollectorTimeout);

         _log.log(Level.INFO, "WFU waiting");
         updateSet = propFilter.getPropertyCollector().waitForUpdatesEx(version, waitOptions);
         _log.log(Level.INFO, "WFU us= " + updateSet);

      } catch (Exception e) {
         _log.log(Level.INFO, "Unexpected exception waiting for VC property change" + e);
      }
      return updateSet;
   }

   private void cleanupWaitForUpdates(PropertyFilter propFilter) {
      propFilter.cleanup();
   }


   private void parseExtraConfig(String key, Object value) {
      if (key.startsWith(VHM_EXTRA_CONFIG_PREFIX)) {
         _log.log(Level.INFO, "PEC key:val = " + key + " : " + value);
         /*
         if (key.equals(VHM_EXTRA_CONFIG_UUID)) {
            if (value.equals("SERENGETI-vApp-m4-jan14-975086")) {
               //_log.log(Level.INFO, "pVoH name= " + mrap._name + " moref= " + mrap._moref.getValue());
               vmInfo.vmRef = mrap._moref;
               vmInfo.name = mrap._name;
            }
         } else if (key.equals(VHM_EXTRA_CONFIG_MASTER_UUID)) {
            vmInfo.masterUuid = value;
         } else if (key.equals(VHM_EXTRA_CONFIG_MASTER_MOREF)) {
            vmInfo.masterMoRefString = value;
         } else if (key.equals(VHM_EXTRA_CONFIG_ELASTIC)) {
            vmInfo.elastic = value.equalsIgnoreCase("true");
         } else if (key.equals(VHM_EXTRA_CONFIG_AUTOMATION_ENABLE)) {
            automationEnabled = value.equalsIgnoreCase("true");
         } else if (key.equals(VHM_EXTRA_CONFIG_AUTOMATION_MIN_INSTANCES)) {
            minInstances = value;
         }
         */
      }
   }
   
   private void parseObjUpdate(ObjectUpdate obj, String[] props) {
      Kind kind = obj.getKind();
      _log.log(Level.INFO, "Pobj kind= " + kind + " obj= " + obj.getObj().getValue());
      if (kind == Kind.modify || kind == Kind.enter || kind == Kind.leave) {
         for (Change pc : obj.getChangeSet()) {
            String pcName = pc.getName();
            Object pcValue = pc.getVal();
            for (String prop : props) {
               int lastIndex = pcName.lastIndexOf(prop);
               if (lastIndex >= 0) {
                  _log.log(Level.INFO, "Pobj prop= " + pcName + " val= " + pcValue);
                  if (prop.equals("config.extraConfig")) {
                     /*
                      * Need to handle two cases.
                      * 1) extraConfig returned as an array (pcName == config.extraConfig)
                      * 2) individual extraConfig entries (pcName = config.extraConfig["xxx"].value)
                      */
                     if (pcName.equals("config.extraConfig")) {
                        OptionValue[] ecl = (OptionValue[]) pcValue;
                        for (OptionValue ec : ecl) {
                           parseExtraConfig(ec.getKey(), ec.getValue());
                        }
                     } else {
                        String [] parts = pcName.split("\"",3);
                        if (parts.length > 1) {
                           _log.log(Level.INFO, "Pobj key = " + parts[1]);
                           parseExtraConfig(parts[1], pcValue);
                        }
                     }
                  }
               }
            }
         }
      }
   }

   public void pcVMsInFolder(Client vcClient, Folder folder) {
      String [] props = {"name", "config.extraConfig", "runtime.powerState", "runtime.host"};
      
      PropertyFilter propFilter = setupWaitForUpdates(vcClient, folder, typeVM, props);

      String version = "";
      while (true) {
         UpdateSet updateSet = callWaitForUpdates(propFilter, version);

         if (updateSet != null) {
            version = updateSet.getVersion();
            FilterUpdate[] updates = updateSet.getFilterSet();

            _log.log(Level.INFO, "WFU version= " + version + " fs= " + updates);
            if (updates != null) {
               for (FilterUpdate pfu : updates) {
                  ObjectUpdate[] objectSet = pfu.getObjectSet();

                  for (ObjectUpdate obj : objectSet) {
                     parseObjUpdate(obj, props);
                  }
               }

            }
         }
      }
      
      // XXX commenting out because this function doesn't return yet 
      // propFilter.cleanup();
   }

   private boolean waitForTask(Task task) {
      PropertyFilter propFilter = new PropertyFilter(defaultClient, task);
      try {
         propFilter.setPropsToFilter(new String [] {TASK_INFO_STATE });
         UpdateSet updateSet = null;
         String version = "";

         WaitOptions waitOptions = new WaitOptions();
         waitOptions.setMaxWaitSeconds(propertyCollectorTimeout);

         while (true) {

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
                                    return false;
                                 } else if (state == TaskInfo.State.success) {
                                    return true;
                                 }
                              }
                           }
                        }
                     }
                  }
               }
            }
         }

      } catch (Exception e) {
         _log.log(Level.INFO, "Unexpected exception waiting for task completion" + e);
      } finally {
         propFilter.cleanup();
      }
      return false;
   }

   
   public void testPC(Client vcClient, String baseFolderName) {
      try {
         Folder f = getFolderForName(baseFolderName);
         _log.log(Level.INFO, "TPC f.name = " + f.getName());
         _log.log(Level.INFO, "TPC f = " + f);
         pcVMsInFolder(vcClient, f);

         
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
         
    
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }
   
}
