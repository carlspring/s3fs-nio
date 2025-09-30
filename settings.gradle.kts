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
    id("com.gradle.develocity") version "4.2"
}

develocity {
    buildScan {
        termsOfUseUrl = "https://gradle.com/terms-of-service"
        termsOfUseAgree = "yes"

        // Automatically publish scans in CI
        publishing.onlyIf { !System.getenv("CI").isNullOrEmpty() }

        capture {
            // https://docs.gradle.com/enterprise/gradle-plugin/current/#capturing_resource_usage
            resourceUsage.set(true)
            buildLogging.set(false)
            testLogging.set(false)
        }

        obfuscation {
            username { _ -> "__redacted__" }
            ipAddresses { addresses -> addresses.map { _ -> "0.0.0.0" } }
            externalProcessName { processName -> "__redacted__" }
        }

        // Add custom values
        value("Project Name", rootProject.name)
        value("Build Branch", System.getenv("GITHUB_REF")?.replace("refs/heads/", "") ?: "N/A")
        value("Build Commit", System.getenv("GITHUB_SHA") ?: "N/A")

        val ciLink = System.getenv("GITHUB_ACTIONS")?.let { "https://github.com/${System.getenv("GITHUB_REPOSITORY")}/actions/runs/${System.getenv("GITHUB_RUN_ID")}" } ?: "N/A"
        // Add tags to the build scan
        tag("CI")
        tag(System.getProperty("os.name"))
        // Add links to related information
        if(ciLink != "N/A") {
            link("GitHub Action", ciLink)
        } else {
            tag("local dev build")
        }
    }
}
