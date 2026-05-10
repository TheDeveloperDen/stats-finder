
plugins {
    kotlin("jvm") version "2.3.0"
    application
    idea
}

group = "org.developerden"
version = "1.0-SNAPSHOT"


kotlin {
    jvmToolchain(25)
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("net.dv8tion:JDA:6.2.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:1.6.4")

    implementation("org.duckdb:duckdb_jdbc:1.4.3.0")

    implementation("org.slf4j:slf4j-api:+")
    implementation("ch.qos.logback:logback-core:+")
    implementation("ch.qos.logback:logback-classic:+")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}


application {
    mainClass.set("MainKt")
}