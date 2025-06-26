plugins {
    kotlin("jvm") version "2.2.0"
}

group = "com.serranofp"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.exposed:exposed-core:1.0.0-beta-2")
    implementation("org.jetbrains.exposed:exposed-dao:1.0.0-beta-2")
    implementation("org.jetbrains.exposed:exposed-jdbc:1.0.0-beta-2")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}