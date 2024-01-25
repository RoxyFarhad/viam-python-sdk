from typing import Mapping, Optional
import typing
import grpclib
from viam.rpc.dial import DialOptions, Credentials
from viam.app.viam_client import ViamClient, AppClient
from viam.app.app_client import Location, Robot, RobotPart
import os
import subprocess
import asyncio
import json
import time

default_config = {
  "webhooks": [
    {
      "event": {
        "attributes": {
          "seconds_between_notifications": 60,
        },
        "type": "part_online"
      },
      "url": "https://us-central1-staging-cloud-web-app.cloudfunctions.net/app-3198-2"
    }
  ],
  "components": [],
  "agent_config": {
    "subsystems": {
      "agent-networking": {
        "release_channel": "stable",
        "pin_version": "",
        "pin_url": "",
        "disable": False
      },
      "viam-agent": {
        "disable": False,
        "release_channel": "stable",
        "pin_version": "",
        "pin_url": ""
      },
      "viam-server": {
        "release_channel": "stable",
        "pin_version": "",
        "pin_url": "",
        "disable": False
      }
    }
  }
}

class ViamAppClient:
    
    def __init__(self, ac: AppClient) -> None:
        self.ac = ac
    
    async def get_location(self) -> Location:
        loc = await self.ac.get_location(location_id="ryn0q0ug3j")
        return loc 

    async def get_or_create_robot(self, location_id: str, robot_name: str) -> str:
        
        robots: list[Robot] = await self.ac.list_robots(location_id=location_id)
        for robot in robots:
            if robot.name == robot_name:
                return robot.id

        robot_id = await self.ac.new_robot(name=robot_name, location_id=location_id)
        return robot_id

    async def create_robot_part(self, robot_id: str, part_name: str) -> str:
        try:
            return await self.ac.new_robot_part(robot_id=robot_id, part_name=part_name)
        except grpclib.exceptions.GRPCError as e:
            if e.message is not None and "already a part with that name" in e.message:
                raise Exception("Duplicate Robot Part Name")
            return ""

    async def get_robot_part_config(self, robot_part_id: str) -> Mapping[str, typing.Any]:
        robot_part = await self.ac.get_robot_part(robot_part_id=robot_part_id)
        return robot_part.robot_config or {}
    
    async def get_robot_part(self, robot_id: str, part_name: str) -> RobotPart:
        robot_parts: list[RobotPart] = await self.ac.get_robot_parts(robot_id=robot_id)
        for part in robot_parts:
            if part.name == part_name:
                return part
        
        raise Exception("there is no part name {} on robot with ID: {}".format(part_name, robot_id))
    
    async def delete_robot_part(self, robot_part_id: str):
        try:
            await self.ac.delete_robot_part(robot_part_id=robot_part_id)
        except Exception as e:
            print("error deleting part {}: {}".format(robot_part_id, e))
    
    async def delete_robot(self, robot_id: str):
        try:
            await self.ac.delete_robot(robot_id=robot_id)
        except Exception as e:
            print("error deleting robot {}: {}".format(robot_id, e))

    async def get_or_create_robot_part_and_set_config(self, 
        robot_id: str, 
        part_name: str, 
        config: Mapping[str, typing.Any]
    ) -> Optional[RobotPart]:
        robot_part_id = ""
        try:
            robot_part = await self.get_robot_part(robot_id=robot_id, part_name=part_name)
            robot_part_id = robot_part.id
        except Exception as e:
            try:
                robot_part_id = await self.create_robot_part(robot_id=robot_id, part_name=part_name)
            except Exception as e:
                print("exception created robot part: {} skipping: ".format(part_name), e)
                return

        part = await self.ac.update_robot_part(robot_part_id=robot_part_id, robot_config=config, name=part_name)
        return part
    
            
# returns the filename to exec viam-server on
def create_file_to_write_config(robot_part_id: str, secret: str) -> str:
    cloud_config: Mapping[str, typing.Any] = {}
    cloud_config["cloud"] = {
        "app_address":"https://app.viam.dev:443",
        "id": robot_part_id,
        "secret": secret,
    }
    filepath = os.path.relpath("./configs")
    filename = "{}/{}-config.json".format(filepath, robot_part_id)
    json_object = json.dumps(cloud_config, indent=2)
    with open(filename, "w") as f:
        f.write(json_object)
    
    return filename

async def cleanup(ac: ViamAppClient, loc_id: str):
    
    # delete all robot parts / robots in the location
    
    robots = await ac.ac.list_robots(location_id=loc_id)
    for robot in robots:
        
        parts = await ac.ac.get_robot_parts(robot_id=robot.id)
        for part in parts:
            await ac.ac.delete_robot_part(robot_part_id=part.id)
        
        await ac.ac.delete_robot(robot_id=robot.id)

    return

async def connect() -> ViamClient:
    dial_options = DialOptions.with_api_key(api_key="", api_key_id="")
    return await ViamClient.create_from_dial_options(dial_options, app_url="")

async def main(loop):
    viam_client = await connect()
    ac = ViamAppClient(ac=viam_client.app_client)
    loc = await ac.get_location()
    
    
    ROBOT_COUNT = 2
    ROBOT_PART_COUNT = 100
    
    all_processes: list[tuple[str, subprocess.Popen[typing.Any]]] = []
    all_robot_part_ids = []
    all_robot_ids = []
    
    
    for i in range(ROBOT_COUNT):
        robot_id = await ac.get_or_create_robot(location_id=loc.id, robot_name="robot-{}".format(i))
        all_robot_ids.append(robot_id)
        bind_address = (8 - i) * 1000
        for j in range(ROBOT_PART_COUNT):
            try:
                part_bind_address = bind_address + j
                part_conig = default_config
                part_conig["network"] = {
                    "bind_address": ":{}".format(part_bind_address)
                }

                new_part = await ac.get_or_create_robot_part_and_set_config(robot_id=robot_id, part_name="robot-{}-{}".format(i, j), config=part_conig)
                if new_part is None:
                    continue
                
                file_name = create_file_to_write_config(robot_part_id=new_part.id, secret=new_part.secret)
                
                print("starting viam-server on robot: {}".format(new_part.id))
                
                f = open("./logs.txt", "w")
                process = subprocess.Popen(['viam-server', '--config', file_name], stdout=f)
                all_robot_part_ids.append(new_part.id)
            except Exception as e:
                print("exception in main loop:", e)
                print("skipping for robot part")

    print("all parts are running")
    time.sleep(60)  
    
    for part_id, process in all_processes:
        print("killing part_id: {}".format(part_id))
        process.kill()
    
    await cleanup(ac, loc_id=loc.id)

    
    viam_client.close()    

loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop))
loop.close()
