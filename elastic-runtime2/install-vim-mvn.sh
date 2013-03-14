
#    <dependency>
#      <groupId>com.vmware</groupId>
#      <artifactId>vlsiCore</artifactId>
#      <version>1.0.8</version>
#    </dependency>

mvn install:install-file -DgroupId=com.vmware -DartifactId=vlsiCore -Dversion=1.0.8 -Dpackaging=jar -Dfile=lib/vlsi/vlsi-core.jar -DpomFile=lib/vlsi/vlsiPom.xml

mvn install:install-file -DgroupId=com.vmware -DartifactId=vlsiClient -Dversion=1.0.8 -Dpackaging=jar -Dfile=lib/vlsi/vlsi-client.jar

mvn install:install-file -DgroupId=com.vmware -DartifactId=vimVmodl -Dversion=1.0.8 -Dpackaging=jar -Dfile=lib/vlsi/vim-vmodl.jar

mvn install:install-file -DgroupId=com.vmware -DartifactId=queryVmodl -Dversion=1.0.8 -Dpackaging=jar -Dfile=lib/vlsi/query-vmodl.jar

mvn install:install-file -DgroupId=com.vmware -DartifactId=reflectVmodl -Dversion=1.0.8 -Dpackaging=jar -Dfile=lib/vlsi/reflect-vmodl.jar

