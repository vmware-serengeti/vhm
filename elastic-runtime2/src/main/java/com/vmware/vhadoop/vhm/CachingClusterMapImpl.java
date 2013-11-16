package com.vmware.vhadoop.vhm;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterConstantData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMConstantData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMVariableData;

@SuppressWarnings("unchecked")
/* This is a version of ClusterMap that caches results from public methods
 * 
 * Not every ClusterMap method is implemented in this class as some are trivial and not worth caching
 * 
 * The caching works as follows: 
 *   AbstractClusterMap has two maps: One with information about clusters (ClusterInfo map) and one with information about vms (VMInfo map)
 *   The public methods implemented here will access one or both of these maps and other than input parameters to the method,
 *     this is their only other input. 
 *   Therefore, any time a change is made to one of these maps or an entry within it, all of the cached data for methods that use that map
 *     as input data should be flushed. Changes to the maps are tracked using the VMCollectionUpdateListener and ClusterCollectionUpdateListener
 *     Changes to entries within the maps are tracked using VMUpdateListener and ClusterUpdateListener. 
 *   Note ClusterMap code MUST use get/set methods to modify the state of ClusterInfo and VMInfo objects in order for the tracking to work
 *     so these types are declared in the ClusterMap interface to prevent direct access to internal fields.
 *   Most methods access either the VMInfo map or ClusterInfo map, but some methods access both. To avoid unnecessary flushing, 
 *     there are therefore 3 cache maps that correspond to these 3 types of access pattern.
 *   The caches use the method name and its input parameters as the key and the object returned as the value
 *   The caching works by looking first in the appropriate map. If a cached entry is not found, the equivalent method in BaseClusterMap
 *     is invoked using reflection. The method has the same name as the calling method, with a postfix tag to identify it.
 *   Since each method needs to know which of the 3 cache maps to use, this should not be left up to trial and error. As such, there are
 *     checks performed every time a non-cached method is invoked to ensure that the correct cache map is being used. This is done by tracking
 *     the number of times a VMInfo or ClusterInfo map is accessed during the invocation of the method.
 *   The cache maps are synchronized because ClusterMap has a concurrent read model
 */
public class CachingClusterMapImpl extends BaseClusterMap {
   private static final Logger _log = Logger.getLogger(CachingClusterMapImpl.class.getName());

   /* The 3 cache maps */
   private Map<InputParams, Object> _resultFromVmList = Collections.synchronizedMap(new HashMap<InputParams, Object>());
   private Map<InputParams, Object> _resultFromClusterList = Collections.synchronizedMap(new HashMap<InputParams, Object>());
   private Map<InputParams, Object> _resultFromVmAndClusterList = Collections.synchronizedMap(new HashMap<InputParams, Object>());
   
   /* Methods are cached to avoid multiple reflection lookups */
   private Map<String, Method> _methods = Collections.synchronizedMap(new HashMap<String, Method>());
   
   @Override
   public Set<String> listComputeVMsForCluster(String clusterId) {
      /* The MethodAccessor is a simple trick to allow us to use class.getEnclosingMethod() to avoid hard-coding method names */
      class MethodAccessor {};
      return (Set<String>)getCachedObjectFromVmList(MethodAccessor.class, clusterId);
   }
   
   @Override
   public Set<String> listComputeVMsForClusterAndPowerState(String clusterId, boolean powerState) {
      class MethodAccessor {};
      return (Set<String>)getCachedObjectFromVmList(MethodAccessor.class, clusterId, powerState);
   }
   
   @Override
   public Set<String> listComputeVMsForClusterHostAndPowerState(String clusterId, String hostId, boolean powerState) {
      class MethodAccessor {};
      return (Set<String>)getCachedObjectFromVmList(MethodAccessor.class, clusterId, hostId, powerState);
   }

   @Override
   public Set<String> listComputeVMsForPowerState(boolean powerState) {
      class MethodAccessor {};
      return (Set<String>)getCachedObjectFromVmList(MethodAccessor.class, powerState);
   }

   @Override
   public Set<String> listHostsWithComputeVMsForCluster(String clusterId) {
      class MethodAccessor {};
      return (Set<String>)getCachedObjectFromVmList(MethodAccessor.class, clusterId);
   }

   @Override
   public Map<String, String> getHostIdsForVMs(Set<String> vmsToED) {
      class MethodAccessor {};
      return (Map<String, String>)getCachedObjectFromVmList(MethodAccessor.class, vmsToED);
   }

   @Override
   public Boolean checkPowerStateOfVms(Set<String> vmIds, boolean expectedPowerState) {
      class MethodAccessor {};
      return (Boolean)getCachedObjectFromVmList(MethodAccessor.class, vmIds, expectedPowerState);
   }

   @Override
   public Boolean checkPowerStateOfVm(String vmId, boolean expectedPowerState) {
      class MethodAccessor {};
      return (Boolean)getCachedObjectFromVmList(MethodAccessor.class, vmId, expectedPowerState);
   }

   @Override
   public Map<String, String> getDnsNamesForVMs(Set<String> vmIds) {
      class MethodAccessor {};
      return (Map<String, String>)getCachedObjectFromVmList(MethodAccessor.class, vmIds);
   }

   @Override
   public String getDnsNameForVM(String vmId) {
      class MethodAccessor {};
      return (String)getCachedObjectFromVmList(MethodAccessor.class, vmId);
   }

   @Override
   public Map<String, String> getVmIdsForDnsNames(Set<String> dnsNames) {
      class MethodAccessor {};
      return (Map<String, String>)getCachedObjectFromVmList(MethodAccessor.class, dnsNames);
   }

   @Override
   public String getVmIdForDnsName(String dnsName) {
      class MethodAccessor {};
      return (String)getCachedObjectFromVmList(MethodAccessor.class, dnsName);
   }

   @Override
   public String getClusterIdForFolder(String clusterFolderName) {
      class MethodAccessor {};
      return (String)getCachedObjectFromClusterList(MethodAccessor.class, clusterFolderName);
   }

   @Override
   public String[] getAllClusterIdsForScaleStrategyKey(String key) {
      class MethodAccessor {};
      return (String[])getCachedObjectFromVmAndClusterList(MethodAccessor.class, key);
   }

   @Override
   public String getScaleStrategyKey(String clusterId) {
      class MethodAccessor {};
      return (String)getCachedObjectFromVmAndClusterList(MethodAccessor.class, clusterId);
   }

   @Override
   public HadoopClusterInfo getHadoopInfoForCluster(String clusterId) {
      class MethodAccessor {};
      return (HadoopClusterInfo)getCachedObjectFromVmAndClusterList(MethodAccessor.class, clusterId);
   }
   
   @Override
   public String getMasterVmIdForCluster(String clusterId) {
      class MethodAccessor {};
      return (String)getCachedObjectFromClusterList(MethodAccessor.class, clusterId);
   }

   @Override
   /* We override this method so that we can add the VMUpdateListener to each VMInfo */
   VMInfo createVMInfo(String moRef, VMConstantData constantData,
         VMVariableData variableData, String clusterId) {
      VMInfo result = super.createVMInfo(moRef, constantData, variableData, clusterId);
      result.setUpdateListener(new VMUpdateListener() {
         @Override
         public void updatingVM(String moRef) {
            resetVMCache(false);
         }
      });
      return result;
   }
   
   @Override
   /* We override this method so that we can add the ClusterUpdateListener to each ClusterInfo */
   ClusterInfo createClusterInfo(String clusterId, SerengetiClusterConstantData constantData) {
      ClusterInfo result = super.createClusterInfo(clusterId, constantData);
      result.setUpdateListener(new ClusterUpdateListener() {
         @Override
         public void updatingCluster(String clusterId) {
            resetClusterCache(false);
         }
      });
      return result;
   }

   /* Simple key type used for indexing cache entries */
   private class InputParams {
      List<Object> _params = new ArrayList<Object>();
      String _methodName;
      
      InputParams(String methodName, Object... objects) {
         _params = Arrays.asList(objects);
         _methodName = methodName;
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + getOuterType().hashCode();
         result = prime * result + ((_methodName == null) ? 0 : _methodName.hashCode());
         result = prime * result + ((_params == null) ? 0 : _params.hashCode());
         return result;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj)
            return true;
         if (obj == null)
            return false;
         if (getClass() != obj.getClass())
            return false;
         InputParams other = (InputParams) obj;
         if (!getOuterType().equals(other.getOuterType()))
            return false;
         if (_methodName == null) {
            if (other._methodName != null)
               return false;
         } else if (!_methodName.equals(other._methodName))
            return false;
         if (_params == null) {
            if (other._params != null)
               return false;
         } else if (!_params.equals(other._params))
            return false;
         return true;
      }

      private CachingClusterMapImpl getOuterType() {
         return CachingClusterMapImpl.this;
      }

   }

   /* Here's where we add the VMCollectionUpdateListener and ClusterCollectionUpdateListener */
   public CachingClusterMapImpl(ExtraInfoToClusterMapper mapper) {
      super(mapper);
      setVMCollectionUpdateListener(new VMCollectionUpdateListener() {
         @Override
         public void updatingVMCollection() {
            resetVMCache(true);
         }
      });
      setClusterCollectionUpdateListener(new ClusterCollectionUpdateListener() {
         @Override
         public void updatingClusterCollection() {
            resetClusterCache(true);
         }
      });
   }
   
   private void resetVMCache(boolean updatingCollection) {
      _log.finer("Resetting VM cache");
      _resultFromVmList.clear();
      _resultFromVmAndClusterList.clear();
   }

   private void resetClusterCache(boolean updatingCollection) {
      _log.finer("Resetting Cluster cache");
      _resultFromClusterList.clear();
      _resultFromVmAndClusterList.clear();
   }

   /* Either return the cached result, or invoke the Base method from the superclass */
   private Object getCachedObjectFromList(Map<InputParams, Object> inputMap, Class<?> methodAccessor, Object... args) {
      String methodName = methodAccessor.getEnclosingMethod().getName();
      InputParams params = new InputParams(methodAccessor.getEnclosingMethod().getName(), args);
      Object result = inputMap.get(params);
      if (result == null) {
         try {
            Method method = getSuperclassMethod(methodAccessor);
            result = method.invoke(this, args);
         } catch (Exception e) {
            _log.log(Level.SEVERE, "Unexpected exception invoking cached method", e);
         }
         if (result != null) {
            inputMap.put(params, result);
         }
      } else {
         _log.finer("Returning cached result from "+methodName);
      }
      return result;
   }

   private Object getCachedObjectFromVmList(Class<?> methodAccessor, Object... args) {
      DataCheck dataCheck = _dataCheckMap.get(Thread.currentThread().getName());
      int clusterAccessCount = 0;
      if (dataCheck != null) {
         clusterAccessCount = dataCheck._clusterInfoMapAccessCount;
      }
      Object result = getCachedObjectFromList(_resultFromVmList, methodAccessor, args);
      /* Double-check that a method purporting to only access the VMInfo map doesn't read the ClusterInfo map */
      if ((dataCheck != null) && (clusterAccessCount != dataCheck._clusterInfoMapAccessCount)) {
         _log.severe("Cached method claiming to only access vm data attempted to access ClusterInfo map!");
      }
      return result;
   }

   private Object getCachedObjectFromClusterList(Class<?> methodAccessor, Object... args) {
      DataCheck dataCheck = _dataCheckMap.get(Thread.currentThread().getName());
      int vmAccessCount = 0;
      if (dataCheck != null) {
         vmAccessCount = dataCheck._vmInfoMapAccessCount;
      }
      Object result = getCachedObjectFromList(_resultFromClusterList, methodAccessor, args);
      /* Double-check that a method purporting to only access the ClusterInfo map doesn't access the VMInfo map */
      if ((dataCheck != null) && (vmAccessCount != dataCheck._vmInfoMapAccessCount)) {
         _log.severe("Cached method claiming to only access cluster data attempted to access VMInfo map!");
      }
      return result;
   }

   /* Objects that access vm and cluster data have their own cache which is cleared whenever either changes */
   private Object getCachedObjectFromVmAndClusterList(Class<?> methodAccessor, Object... args) {
      return getCachedObjectFromList(_resultFromVmAndClusterList, methodAccessor, args);
   }

   /* Caches a Method object representing the base method in the superclass, indexed by method signature */
   private Method getSuperclassMethod(Class<?> methodAccessor) {
      Method method = methodAccessor.getEnclosingMethod();
      Method result = _methods.get(method.toString());
      if (result == null) {
         try {
            Class<?>[] argsList = methodAccessor.getEnclosingMethod().getParameterTypes();
            result = getClass().getSuperclass().getDeclaredMethod(method.getName()+BASE_METHOD_POSTIFX, argsList);
            result.setAccessible(true);
            _methods.put(method.toString(), result);
         } catch (Exception e) {
            _log.log(Level.SEVERE, "Unexpected exception getting cached method", e);
         }
      }
      return result;
   }
}
