val aeronVersion: String by project
val artioVersion: String by project
val agronaVersion: String by project

dependencies {
    implementation(project(":messages"))
    implementation("io.aeron:aeron-cluster:$aeronVersion")
    implementation("uk.co.real-logic:artio-core:$artioVersion")
    implementation("org.agrona:agrona:$agronaVersion")
}
