import com.adarshr.gradle.testlogger.theme.ThemeType
import java.text.SimpleDateFormat
import java.util.*

plugins {
    base
    `java-library`
    jacoco
    `jacoco-report-aggregation`
    `maven-publish`
    signing
    id("com.adarshr.test-logger") version "3.2.0"
    id("org.sonarqube") version "4.0.0.2929"
}

allprojects {
    repositories {
        mavenLocal()
        // Allows you to specify your own repository manager instance.
        if (project.hasProperty("s3fs.proxy.url")) {
            maven {
                url = project.findProperty("s3fs.proxy.url")?.let { uri(it) }!!
            }
        }
        // Fallback to maven central
        mavenCentral()
    }
}

group = "org.carlspring.cloud.aws"

val isReleaseVersion = !version.toString().lowercase(Locale.getDefault()).endsWith("snapshot")

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    api(platform("software.amazon.awssdk:bom:2.16.28")) {
        // Change the `AWS_VERSION` in ./docs/mkocs.yaml file.
    }
    api("software.amazon.awssdk:s3") {
        exclude("commons-logging", "commons-logging")
    }
    api("software.amazon.awssdk:apache-client")
    api("com.google.guava:guava:30.1.1-jre")
    api("org.apache.tika:tika-core:2.5.0") {
        exclude("org.slf4j", "slf4j-api")
    }
    api("com.google.code.findbugs:jsr305:3.0.2")

    testImplementation("ch.qos.logback:logback-classic:1.2.9")
    testImplementation("org.junit.jupiter:junit-jupiter:5.8.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.0")
    testImplementation("org.apache.commons:commons-lang3:3.12.0")
    testImplementation("com.github.marschall:zipfilesystem-standalone:1.0.1")
    testImplementation("com.github.marschall:memoryfilesystem:2.1.0")
    testImplementation("org.mockito:mockito-core:3.9.0")
    testImplementation("org.mockito:mockito-inline:3.9.0")
    testImplementation("org.mockito:mockito-junit-jupiter:3.9.0")
    testImplementation("org.testcontainers:testcontainers:1.18.1")
    testImplementation("org.testcontainers:testcontainers:1.18.1")
    testImplementation("org.assertj:assertj-core:3.24.2")
}

configure<com.adarshr.gradle.testlogger.TestLoggerExtension> {
    theme = ThemeType.MOCHA_PARALLEL
    showExceptions = true
    showStackTraces = true
    showFullStackTraces = true
    showCauses = true
    slowThreshold = 2000
    showSummary = true
    showSimpleNames = false
    showPassed = true
    showSkipped = true
    showFailed = true
    showStandardStreams = false
    showPassedStandardStreams = true
    showSkippedStandardStreams = true
    showFailedStandardStreams = true
    logLevel = LogLevel.WARN
}

sonarqube {
    properties {
        property("sonar.host.url", "https://sonarcloud.io")
        property("sonar.projectKey", "carlspring_s3fs-nio")
        property("sonar.organization", "carlspring")
        property("sonar.login", project.findProperty("s3fs.publish.sonar.login") ?: System.getenv("S3FS_PUBLISH_SONAR_LOGIN") ?: System.getenv("SONAR_TOKEN") as String)
        property("sonar.java.target", java.targetCompatibility)
    }
}

tasks {

    withType<JavaCompile>().configureEach {
        options.isFork = true
        options.encoding = "UTF-8"
    }

    withType<Javadoc> {
        options.encoding = "UTF-8"
    }

    named<Jar>("jar") {
        val attrs = HashMap<String, String?>()
        val sdf = SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz")
        sdf.timeZone = TimeZone.getTimeZone("UTC")
        attrs["Build-Date"] = sdf.format(Date())
        attrs["Build-JDK"] = System.getProperty("java.version")
        attrs["Build-Gradle"] = project.gradle.gradleVersion
        attrs["Build-OS"] = System.getProperty("os.name")
        attrs["Build-CI"] = System.getProperty("CI", "false")
        attrs["Version"] = version.toString()
        manifest.attributes(attrs)
    }

    named<Task>("build") {
        if (!project.hasProperty("localPublish") || project.findProperty("localPublish") == "true") {
            finalizedBy(named("publishToMavenLocal"))
        }
    }

    named<Task>("jacocoTestReport") {
        group = "jacoco"
        dependsOn(named("test")) // tests are required to run before generating the report
        finalizedBy(named("testCodeCoverageReport"))
    }

    named<Task>("jacocoTestCoverageVerification") {
        group = "jacoco"
    }

    named<Task>("testCodeCoverageReport") {
        group = "jacoco"
    }

    named<Task>("sonar") {
        group = "sonar"
    }

    named<Task>("sonarqube") {
        group = "sonar"
    }

    named<Test>("test") {
        description = "Run unit tests"
        useJUnitPlatform {
            filter {
                excludeTestsMatching("*IT")
            }
        }
    }

    withType<Test> {
        defaultCharacterEncoding = "UTF-8"
    }

    create<Test>("it-s3") {
        group = "verification"
        description = "Run integration tests using S3"
        useJUnitPlatform {
            filter {
                includeTestsMatching("*IT")
                includeTags("it-s3")
            }
        }
        mustRunAfter(named("test"))
    }

    // TODO: There are some problems with using minio that overcomplicate the setup.
    //       For the time being we'll be disabling it until we figure out the best path forward.
//    create<Test>("it-minio") {
//        group = "verification"
//        description = "Run integration tests using Minio"
//        useJUnitPlatform {
//            filter {
//                includeTestsMatching("*IT")
//                includeTags("it-minio")
//            }
//        }
//    }

    named<Task>("check") {
        dependsOn(named("it-s3"))
    }

    named<Jar>("jar") {
        val attrs = HashMap<String, String?>()
        val sdf = SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz")
        sdf.timeZone = TimeZone.getTimeZone("UTC")
        attrs["Build-Date"] = sdf.format(Date())
        attrs["Build-JDK"] = System.getProperty("java.version")
        attrs["Build-Gradle"] = project.gradle.gradleVersion
        attrs["Build-OS"] = System.getProperty("os.name")
        attrs["Build-Automatic"] = System.getProperty("CI", "false")
        manifest.attributes(attrs)
        exclude("**/*.RSA", "**/*.SF", "**/*.DSA", "**/amazon-*.properties")
    }

    withType<Sign> {
        onlyIf {
            (project.hasProperty("withSignature") && project.findProperty("withSignature") == "true") ||
            (isReleaseVersion && gradle.taskGraph.hasTask("publish"))
        }
    }

}

signing {
    useGpgCmd()
    sign(publishing.publications)
}

publishing {
    repositories {
        maven {
            // Allow deploying to a custom repository (for testing purposes)
            val publishInternally = project.findProperty("s3fs.publish.type")?.toString() == "internal";

            var repositoryUrl: String;

            if (isReleaseVersion) {
                val releaseMavenCentral = "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
                val releaseInternal = (project.findProperty("s3fs.publish.internal.release") ?: System.getenv("S3FS_PUBLISH_INTERNAL_RELEASE") ?: "") as String
                repositoryUrl = if (releaseInternal != "" && publishInternally) releaseInternal else releaseMavenCentral
            } else {
                val snapshotMavenCentral = "https://oss.sonatype.org/content/repositories/snapshots/"
                val snapshotInternal = (project.findProperty("s3fs.publish.internal.snapshot") ?: System.getenv("S3FS_PUBLISH_INTERNAL_SNAPSHOT") ?: "") as String
                repositoryUrl = if (snapshotInternal != "" && publishInternally) snapshotInternal else snapshotMavenCentral
            }

            url = repositoryUrl.let { uri(it) }

            authentication {
                create<BasicAuthentication>("basic")
            }
            credentials {
                val mavenCentralUser = (project.findProperty("s3fs.publish.sonatype.user") ?: System.getenv("S3FS_PUBLISH_SONATYPE_USER") ?: "") as String
                val mavenCentralPass = (project.findProperty("s3fs.publish.sonatype.pass") ?: System.getenv("S3FS_PUBLISH_SONATYPE_PASS") ?: "") as String
                val internalUser = (project.findProperty("s3fs.publish.internal.user") ?: System.getenv("S3FS_PUBLISH_INTERNAL_USER") ?: "") as String
                val internalPass = (project.findProperty("s3fs.publish.internal.pass") ?: System.getenv("S3FS_PUBLISH_INTERNAL_PASS") ?: "") as String
                username = if (publishInternally) internalUser else mavenCentralUser
                password = if (publishInternally) internalPass else mavenCentralPass
            }

        }
    }
    publications {
        create<MavenPublication>("pluginMaven") {
            from(components.named("java").get())

            groupId = project.group as String?
            artifactId = project.name
            version = project.version as String?

            //println("Publishing: ${groupId}:${artifactId}:${version}" )

            withBuildIdentifier()

            versionMapping {
                usage("java-api") {
                    fromResolutionOf("runtimeClasspath")
                }
                usage("java-runtime") {
                    fromResolutionResult()
                }
            }

            pom {
                name.set("S3FS NIO")
                description.set("A Java NIO FileSystem Provider for Amazon AWS S3")
                url.set("https://github.com/carlspring/s3fs-nio")
                issueManagement {
                    url.set("https://github.com/carlspring/s3fs-nio/issues")
                }
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                    license {
                        name.set("MIT License")
                        url.set("http://www.opensource.org/licenses/mit-license.php")
                    }
                }
                developers {
                    developer {
                        id.set("carlspring")
                        name.set("Martin Todorov")
                        email.set("carlspring@gmail.com")
                        organization.set("Carlspring Consulting & Development Ltd.")
                        url.set("https://github.com/carlspring")
                    }
                    developer {
                        id.set("steve-todorov")
                        name.set("Steve Todorov")
                        email.set("steve.todorov@carlspring.com")
                        organization.set("Carlspring Consulting & Development Ltd.")
                        url.set("https://github.com/steve-todorov")
                    }
                    developer {
                        id.set("jcustovic")
                        name.set("Jan Čustović")
                        email.set("jan.custovic@gmail.com")
                    }
                    developer {
                        id.set("heikkipora")
                        name.set("Heikki Pora")
                        email.set("heikki.pora@gmail.com")
                    }
                    developer {
                        id.set("pditommaso")
                        name.set("Paolo Di Tommaso")
                        email.set("paolo.ditommaso@gmail.com")
                    }
                    developer {
                        id.set("sbeimin")
                        name.set("Syte Beimin")
                        email.set("syte.beimin@gmail.com")
                    }
                    developer {
                        id.set("jarnaiz")
                        name.set("Javier Arnáiz")
                        email.set("arnaix@gmail.com")
                    }
                    developer {
                        id.set("martint")
                        name.set("Martin Traverso")
                        email.set("mtraverso@gmail.com")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/carlspring/s3fs-nio.git")
                    developerConnection.set("scm:git:ssh://github.com/carlspring/s3fs-nio.git")
                    url.set("https://github.com/carlspring/s3fs-nio")
                }
            }
        }
    }
}
