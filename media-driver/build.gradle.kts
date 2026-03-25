plugins {
    java
    application
}

val aeronVersion: String by project
val agronaVersion: String by project

dependencies {
    implementation("io.aeron:aeron-driver:$aeronVersion")
    implementation("io.aeron:aeron-archive:$aeronVersion")
    implementation("org.agrona:agrona:$agronaVersion")
}

application {
    mainClass.set("com.trading.engine.media.driver.MediaDriverLauncher")
}
