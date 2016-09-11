package ru.semiot.platform.smartclient;

public class Device {

  String building;
  String system_id;
  String type;
  
  public Device(String building, String system_id, String type) {
    this.building = building;
    this.system_id = system_id;
    this.type = type;
  }
  
  public String getBuilding() {
    return building;
  }
  
  public String getSystemId() {
    return system_id;
  }
  
  public String getType() {
    return type;
  }
  
}
