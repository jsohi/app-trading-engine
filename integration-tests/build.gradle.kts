val artioVersion: String by project

dependencies {
    testImplementation(project(":launcher"))
    testImplementation(project(":query-service"))
    testImplementation("uk.co.real-logic:artio-core:$artioVersion")
}
