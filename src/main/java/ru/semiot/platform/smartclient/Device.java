package ru.semiot.platform.smartclient;

public class Device {

  private final String building;
  private final String systemId;
  private final String type;
  
  public Device(String building, String systemId, String type) {
    this.building = building;
    this.systemId = systemId;
    this.type = type;
  }
  
  public String getBuilding() {
    return building;
  }
  
  public String getSystemId() {
    return systemId;
  }
  
  public String getType() {
    return type;
  }
  
}
