package com.vmware.vhadoop.adaptor.vc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.soap.SOAPFaultException;

import com.vmware.vim25.DynamicProperty;
import com.vmware.vim25.InvalidLocaleFaultMsg;
import com.vmware.vim25.InvalidLoginFaultMsg;
import com.vmware.vim25.InvalidPropertyFaultMsg;
import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.ObjectContent;
import com.vmware.vim25.ObjectSpec;
import com.vmware.vim25.ObjectUpdate;
import com.vmware.vim25.ObjectUpdateKind;
import com.vmware.vim25.PropertyChange;
import com.vmware.vim25.PropertyFilterSpec;
import com.vmware.vim25.PropertyFilterUpdate;
import com.vmware.vim25.PropertySpec;
import com.vmware.vim25.RuntimeFaultFaultMsg;
import com.vmware.vim25.ServiceContent;
import com.vmware.vim25.TraversalSpec;
import com.vmware.vim25.UpdateSet;
import com.vmware.vim25.VimPortType;
import com.vmware.vim25.VimService;

/**
 * Designed to encapsulate a lot of the low level detail of interacting with the JAX-WS VC API
 * Any particularly verbose utility code can go in VCUtils to keep this tidy
 * 
 * @author bcorrie
 *
 */
public class VCConnection {

   private static final Logger _log = Logger.getLogger(VCConnection.class.getName());

   interface VCCredentials {
      public String getHostName();
      public String getUserName();
      public String getPassword();
   }
   
   /**
    * Represents a property that we're interesting in seeing a change in.
    * Params: The property, the particular attribute of the property we're expecting to change
    * and the expected attribute values that define a change we're looking for.
    * 
    * @author bcorrie
    *
    */
   protected class WaitProperty {
      String _propToFilter;
      String _attributeToWaitFor;
      Object[] _expectedAttValues;

      public WaitProperty(String propToQuery, String attributeToWaitFor, Object[] expectedAttValues) {
         _propToFilter = propToQuery;
         _attributeToWaitFor = attributeToWaitFor;
         _expectedAttValues = expectedAttValues;
      }
   }
   
   /**
    * Once a property has changed, the result of that is encapsulated as a PropertyChangeResult.
    * We pass back the original definition of the property we were looking for a change in and
    * then the actual value that it changed to.
    * 
    * @author bcorrie
    *
    */
   protected class PropertyChangeResult {
      WaitProperty _property;
      Object _foundResult;

      public PropertyChangeResult(WaitProperty property, Object foundResult) {
         _property = property;
         _foundResult = foundResult;
      }
   }
   
   /**
    * Simple encapsulation of a ManagedObjectReference and a set of properties which the caller uses to build a DTO.
    * Nothing in this class knows about the DTOs as the code should be as generic as possible
    * 
    * @author bcorrie
    *
    */
   class MoRefAndProps {
      String _name;
      ManagedObjectReference _moref;
      Map<String, Object> _properties;
      
      public MoRefAndProps(ManagedObjectReference moref, String name) {
         _moref = moref;
         _name = name;
      }
      
      public void addProperty(String key, Object value) {
         if (_properties == null) {
            _properties = new HashMap<String, Object>();
         }
         _properties.put(key, value);
      }
   }

   /* TODO: Global state is acceptable while this class is being used by a single thread, but
    * in the longer term it would be good to cache some of this state in thread local storage.
    * Before we do this, we should look at which calls are the most expensive and are safe to cache.
    * Ideal scenario: cached connection state is initialized when VHM method is entered, passed around
    * and reused by VCConnection methods and then cleaned up when the VHM method exits.
    */
   private VimService _vimService;
   private VimPortType _vimPort;
   private ServiceContent _serviceContent;
   private VCCredentials _credentials;
   private boolean _connected;

   public VCConnection(VCCredentials credentials) {
      _credentials = credentials;
   }

   @SuppressWarnings("finally")
private boolean testConnection() {
	  if (!_connected) {
		 return false;
	  }
      /* Test the operation of the current connection using the standard simple call for this purpose.
       * Note: Don't use getVimPort() which would recursively try to test the connection.
       * */
      XMLGregorianCalendar vcTime = null;
      try {
         ManagedObjectReference svcInstRef = new ManagedObjectReference();
         svcInstRef.setType("ServiceInstance");
         svcInstRef.setValue("ServiceInstance");
         vcTime = _vimPort.currentTime(svcInstRef);
      } finally {
    	 if (vcTime == null) {
    		 _log.log(Level.SEVERE, "testConnection found VC connection dropped; caller will reconnect");
    	 }
    	 return vcTime != null;
      }
   }

   private String getWsURL() {
      return "https://"+_credentials.getHostName()+"/sdk";
   }

   /**
    * Initialize the connection to VC using the credentials provided
    * 
    * @return true if a successful connection is created, false otherwise
    * @throws SOAPFaultException
    */
   public boolean connect() throws SOAPFaultException {
      if (_connected) {
         if (testConnection()) {
            return true;
         } else {
            /* Refresh */
            _connected = false;
         }
      }

      ManagedObjectReference svcInstRef = new ManagedObjectReference();
      svcInstRef.setType("ServiceInstance");
      svcInstRef.setValue("ServiceInstance");

      _vimService = new VimService();
      _vimPort = _vimService.getVimPort();
      Map<String, Object> ctxt = ((BindingProvider) _vimPort).getRequestContext();

      ctxt.put(BindingProvider.ENDPOINT_ADDRESS_PROPERTY, getWsURL());
      ctxt.put(BindingProvider.SESSION_MAINTAIN_PROPERTY, true);

      try {
         _serviceContent = _vimPort.retrieveServiceContent(svcInstRef);
         _vimPort.login(_serviceContent.getSessionManager(), _credentials.getUserName(), _credentials.getPassword(), null);
         _connected = true;
      } catch (RuntimeFaultFaultMsg e) {
         throw new RuntimeException("Unexpected Connect Exception", e);
      } catch (InvalidLocaleFaultMsg e) {
         throw new RuntimeException("Unexpected Connect Exception", e);
      } catch (InvalidLoginFaultMsg e) {
         throw new RuntimeException("Unexpected Connect Exception", e);
      }
      
      return testConnection();
   }
   
   /* Retry is encapsulated here. We always hand back a valid service content or null */
   protected ServiceContent getServiceContent() {
	  if (connect()) {
         return _serviceContent;
	  }
	  return null;
   }
   
   /* Retry is encapsulated here. We always hand back a valid vim port or null */
   protected VimPortType getVimPort() {
	  if (connect()) {
         return _vimPort;
	  }
	  return null;
   }
   
   public void disconnect(boolean testAsyncDrop) throws SOAPFaultException {
      if (_connected) {
         try {
            getVimPort().logout(getServiceContent().getSessionManager());
            if (!testAsyncDrop) { // for testing connection drop asynchronous to VHM operation.
               _connected = false;
            }
         } catch (RuntimeFaultFaultMsg e) {
            throw new RuntimeException("Unexpected Disconnect Exception", e);
         }
      }
   }
  
   /**
    * For the object represented by the moref, wait until ANY of the wait properties in the array have changed 
    * 
    * @param forObject the object to wait for a property change on
    * @param waitProperties the properties to look for changes in
    * @return a PropertyChangeResult of the first property to change or null if none changed
    */
   /* TODO: It's not clear enough whether this method blocks or just does a query and returns. Assuming blocking */
   public PropertyChangeResult waitForPropertyChange(ManagedObjectReference forObject, WaitProperty[] waitProperties) {
      PropertyChangeResult result = null;
      String version = "";

      PropertyFilter propFilter = new PropertyFilter(forObject);
      for (WaitProperty waitProperty : waitProperties) {
         propFilter.addPropToFilter(waitProperty._propToFilter);
      }
      try {
         ManagedObjectReference propCollector = propFilter.getPropertyCollector();
         /* TODO: Need some timeout mechanism */
         propSearch :
         while (true) {
            UpdateSet updateSet = _vimPort.waitForUpdates(propCollector, version);
            version = updateSet.getVersion();
            
            if (updateSet != null) {
               List<PropertyFilterUpdate> filterSet = updateSet.getFilterSet();
               
               if (filterSet != null) {
                  for (PropertyFilterUpdate pfu : filterSet) {
                     List<ObjectUpdate> objectSet = pfu.getObjectSet();
                     
                     for (ObjectUpdate obj : objectSet) {
                        result = findExpectedPropertyValue(waitProperties, obj);
                        if (result != null) {
                           break propSearch;
                        }
                     }
                  }
               }
            }
         }
         propFilter.cleanup();
      } catch (Exception e) {
         _log.log(Level.SEVERE, "Unexpected exception waiting for VC property change", e);
      }
      return result;
   }
   
   /* Within a Property change set of an ObjectUpdate, look to see if any of the waitProperties
    * in the array are a match. If so, return a PropertyChangeResult. False otherwise. */
   private PropertyChangeResult findExpectedPropertyValue(WaitProperty[] waitProperties, ObjectUpdate obj) {
      ObjectUpdateKind kind = obj.getKind();
      if (kind == ObjectUpdateKind.MODIFY || kind == ObjectUpdateKind.ENTER || kind == ObjectUpdateKind.LEAVE) {
         for (PropertyChange pc : obj.getChangeSet()) {
            String pcName = pc.getName();
            Object pcValue = pc.getVal();
            for (WaitProperty wp : waitProperties) {
               if (pcName.lastIndexOf(wp._attributeToWaitFor) >= 0) {
                  for (Object expected : wp._expectedAttValues) {
                     if (expected.equals(pcValue)) {
                        return new PropertyChangeResult(wp, pcValue);
                     }
                  }
               }
            }
         }
      }
      return null;
   }
   
   /**
    * Query VC for the current value of the property key specified for the object moref.
    * If the property is found, the current value is returned. Null otherwise.
    * 
    * @param forObject the object to query the property on
    * @param statePropToRefresh the key of the property
    * @return the value of the property or null
    */
   public Object refreshStateProperty(ManagedObjectReference forObject, String statePropToRefresh) {
      try {
         PropertyFilter propertyFilter = new PropertyFilter(forObject);
         propertyFilter.addPropToFilter(statePropToRefresh);
         List<ObjectContent> oca = propertyFilter.retrieveProperties();
         if (oca != null) {
            for (ObjectContent oc : oca) {
               List<DynamicProperty> dps = oc.getPropSet();
               if (dps != null) {
                  for (DynamicProperty dp : dps) {
                     if (dp.getName().equals(statePropToRefresh)) {
                        return dp.getVal();
                     }
                  }
               }
            }
         }
      } catch (Exception e) {
         return null;
      }
      return null;
   }
   
   /* Look for VC entities in a particular root moref (typically a folder)
    * The state properties to retrieve define state on the entity that we're interested in caching
    * The values of the state properties are passed back and are built into a DTO object
    */
   private List<MoRefAndProps> findObjectsFromRoot(
         ManagedObjectReference rootObject, String type, String restrictToName, String[] statePropsToGet) {
      List<MoRefAndProps> result = new ArrayList<MoRefAndProps>();

      try {
         ManagedObjectReference containerView = _vimPort.createContainerView(_serviceContent.getViewManager(),
               rootObject, Arrays.asList(type), true);
         PropertyFilter propFilter = new PropertyFilter(containerView, type);
         propFilter.addPropToFilter("name");
         if (statePropsToGet != null) {
            for (String prop : statePropsToGet) {
               propFilter.addPropToFilter(prop);
            }
         }

         List<ObjectContent> oca = propFilter.retrieveProperties();

         if (oca != null) {
            for (ObjectContent oc : oca) {
               List<DynamicProperty> dps = oc.getPropSet();
               
               if (dps != null) {
                  MoRefAndProps moRefAndProps = null;
                  Map<String, Object> cachedDps = new HashMap<String, Object>();
                  for (DynamicProperty dp : dps) {
                     cachedDps.put(dp.getName(), dp.getVal());
                     if (dp.getName().equalsIgnoreCase("name")) {
                        String objectName = (String) dp.getVal();
                        if ((restrictToName == null) || restrictToName.equalsIgnoreCase(objectName)) {
                           moRefAndProps = new MoRefAndProps(oc.getObj(), objectName);
                           result.add(moRefAndProps);
                        }
                     }
                  }
                  if ((moRefAndProps != null) && (statePropsToGet != null)) {
                     for (String prop : statePropsToGet) {
                        moRefAndProps.addProperty(prop, cachedDps.get(prop));
                     }
                  }
               }
            }
         }
         propFilter.cleanup();
         getVimPort().destroyView(containerView);
      } catch (Exception e) {
         if (restrictToName != null) {
            _log.log(Level.SEVERE, "Unexpected exception looking for object of name: "+restrictToName, e);
         } else {
            _log.log(Level.SEVERE, "Unexpected exception looking for objects of type: "+type, e);
         }
      }
      return result;
   }

   /**
    * Find all VC objects of a particular type within the scope of a root object
    * 
    * @param rootObject The root object moref, typically a folder
    * @param type The object type
    * @param props The object properties to query
    * @return Object morefs and property key/values for each one, or null
    */
   public MoRefAndProps[] findObjectsFromRoot(ManagedObjectReference rootObject, String type, String[] props) {
      List<MoRefAndProps> result = findObjectsFromRoot(rootObject, type, null, props);
      if (result.size() > 0) {
         return result.toArray(new MoRefAndProps[0]);
      }
      return null;
   }
            
   /**
    * Find a specific VC object by name of a particular type within the scope of a root object
    * 
    * @param rootObject The root object moref, typically a folder
    * @param type The object type
    * @param targetName The object name
    * @param props The object properties to query
    * @return Object moref and the property key/values for it, or null
    */
   public MoRefAndProps findObjectFromRoot(ManagedObjectReference rootObject, String type, String targetName, String[] props) {
      List<MoRefAndProps> result = findObjectsFromRoot(rootObject, type, targetName, props);
      if (result.size() > 0) {
         return result.get(0);
      }
      return null;
   }
   
   /**
    * Property filters are used a lot when querying the JAX-WS API for information about VC entities
    * The code is pretty ugly, so it makes sense to encapsulate it in a utility class.
    * The class is dual-purpose - it can be created with either constructor depending on the need.
    * Properties can then be added to the filter and once that's completed, 
    * retrieveProperties() or getPropertyCollector() can be called, depending on the requirement
    * 
    * @author bcorrie
    *
    */
   private class PropertyFilter {
      ManagedObjectReference _propertyCollector;
      ManagedObjectReference _filterRef;
      PropertySpec _propertySpec;
      PropertyFilterSpec _propertyFilterSpec;
      boolean _initialized = false;
      
      /* TODO: Make sense of these two constructors. Can they be consolidated? */
      public PropertyFilter(ManagedObjectReference containerView, String type) {
         TraversalSpec tSpec = getTraversalSpecFromView();
         ObjectSpec objectSpec = new ObjectSpec();
         objectSpec.setObj(containerView);
         objectSpec.getSelectSet().add(tSpec);
         _propertyFilterSpec = new PropertyFilterSpec();
         _propertyFilterSpec.getObjectSet().add(objectSpec);
         _propertySpec = new PropertySpec();
         _propertySpec.setAll(Boolean.FALSE);
         _propertySpec.setType(type);
      }
      
      public PropertyFilter(ManagedObjectReference moref) {
         ObjectSpec objectSpec = new ObjectSpec();
         objectSpec.setObj(moref);
         objectSpec.setSkip(Boolean.FALSE);
         _propertyFilterSpec = new PropertyFilterSpec();
         _propertyFilterSpec.getObjectSet().add(objectSpec);
         _propertySpec = new PropertySpec();
         _propertySpec.setType(moref.getType());
      }
      
      /* TODO: Could this be called post-init? */
      public void addPropToFilter(String property) {
         _propertySpec.getPathSet().add(property);
      }
   
      private void init() throws RuntimeFaultFaultMsg, InvalidPropertyFaultMsg {
         if (!_initialized) {
            _propertyFilterSpec.getPropSet().add(_propertySpec);
            _propertyCollector = getServiceContent().getPropertyCollector();
         }
         _initialized = true;
      }
      
      public ManagedObjectReference getPropertyCollector() throws RuntimeFaultFaultMsg, InvalidPropertyFaultMsg {
         init();
         if (_filterRef == null) {
            _filterRef = getVimPort().createFilter(_propertyCollector, _propertyFilterSpec, true);
         }
         return _propertyCollector;
      }
      
      public List<ObjectContent> retrieveProperties() throws RuntimeFaultFaultMsg, InvalidPropertyFaultMsg {
         init();
         return getVimPort().retrieveProperties(_propertyCollector, Arrays.asList(_propertyFilterSpec));
      }
      
      public void cleanup() throws RuntimeFaultFaultMsg {
         if (_filterRef != null) {
            getVimPort().destroyPropertyFilter(_filterRef);
         }
      }
   
      private TraversalSpec getTraversalSpecFromView() {
         // Create a traversal spec that starts from the ListView object
         // and traverses to its "view" property containing the managed object references.
   
         TraversalSpec viewToObject = new TraversalSpec();
         viewToObject.setName("viewToObject");
         viewToObject.setType("ContainerView");
         viewToObject.setPath("view");
         viewToObject.setSkip(false);
   
         return viewToObject;
      }
   }
}
