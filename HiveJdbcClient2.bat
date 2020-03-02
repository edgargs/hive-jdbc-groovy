
set JAVA_OPTS=-Dsun.security.krb5.debug=true -Djavax.security.auth.useSubjectCredsOnly=false -Djava.security.krb5.conf=C:\ProgramData\MIT\Kerberos5\krb5.ini
set CP_HIVE=lib\hive-jdbc-uber-2.6.5.0-292.jar

groovy -cp %CP_HIVE%; HiveJDBCSample2.groovy %1 %2