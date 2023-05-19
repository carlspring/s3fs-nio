rootProject.name = "s3fs-nio"

pluginManagement {
    repositories {
        mavenLocal()
        // Allows you to specify your own repository manager instance.
        if(extra.has("s3fs.proxy.url")) {
            maven { url = extra["s3fs.proxy.url"]?.let { uri(it) }!! }
        }
        gradlePluginPortal()
        mavenCentral()
    }
}

plugins {
    id("com.gradle.enterprise") version("3.13")
}

gradleEnterprise {
    buildScan {
        termsOfServiceUrl = "https://gradle.com/terms-of-service"
        termsOfServiceAgree = "yes"
        capture {
            // Capture task inputs.
            isTaskInputFiles = true
        }
        // "auto-accept" license agreements only when running under CI server.
        publishAlwaysIf(!System.getenv("CI").isNullOrEmpty())
    }
}
