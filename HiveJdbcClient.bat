set JDK_HOME="C:\Program Files"\Java\jdk1.8.0_211

set JAVA_OPTS=-Dsun.security.krb5.debug=true -Djavax.security.auth.useSubjectCredsOnly=false -Djava.security.krb5.conf=C:\ProgramData\MIT\Kerberos5\krb5.ini
set CP_HIVE=lib\hive-jdbc-uber-2.6.5.0-292.jar
set CP_HIVE=%CP_HIVE%;lib\jackson-core-2.9.10.jar
set CP_HIVE=%CP_HIVE%;lib\jackson-databind-2.9.10.jar
set CP_HIVE=%CP_HIVE%;lib\jackson-annotations-2.9.10.jar

%JDK_HOME%\bin\javac.exe -cp %CP_HIVE% .\HiveJdbcClient.java
java -cp %CP_HIVE%; %JAVA_OPTS% HiveJdbcClient > script_update.sql