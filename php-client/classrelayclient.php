<?php

/*
    mysql-replication-relay
    Copyright (C) 2023 Freedelity

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

  define ("RCVERSION",0x00010004);
  
  define ("REPRELAY_PING",0X01);
  define ("REPRELAY_AUTH",0X02);
  define ("REPRELAY_ADDFILTER",0X03);
  define ("REPRELAY_POLL",0X04);


  define ("REPRELAYREQ_INSERT",0X01);
  define ("REPRELAYREQ_UPDATE",0X02);
  define ("REPRELAYREQ_DELETE",0X04);

  define ("RCDEBUG",0);

	function hexstr($buf,$_len=0)
	{
		$str="";
    if($_len==0) $len=strlen($buf);
    else $len=$_len; 
		for($i=0;$i<$len;$i++) $str.=sprintf("%02X ",ord($buf[$i]));
		return $str; 
	}

  function FNV1AQ($str)
  {
    $hash=gmp_init("0xCBF29CE484222325",16);
    $fac=gmp_init("0x00000100000001B3");
    $mask=gmp_init("0xFFFFFFFFFFFFFFFF");
    for($i=0;$i<strlen($str);$i++) 
    {
      $hash=((ord($str[$i])^$hash)*$fac)&$mask;
    }
    return $hash;
  }

  class RelayClient {
    
    private $client_id;
    private $client_token;
    private $server_host;
    private $server_port;
    private $socket;
    private $payload;
    
    
    // ==================================================================================================
    
    function __construct() {
      if(RCDEBUG) echo "ReplicationRelayClient:construct\n";
      $client_id=0;
      $client_token=0;
      $server_host="127.0.0.1";
      $server_port=6001;
      $socket=null;      
      $payload="";
    }
    
    // ==================================================================================================
    // (Internal) Send a command to the replication relay (Auth/Ping/AddFilter/Poll)
    function sendcommand($cmd,$subcmd)
    {
      if(RCDEBUG) echo "ReplicationRelayClient:sendcommand($cmd,$subcmd)\n";
      
      $checksum=$cmd+$subcmd+strlen($this->payload)+RCVERSION+$this->client_id+$this->client_token+0+0;
      for($i=0;$i<strlen($this->payload);$i++) $checksum+=ord($this->payload[$i]);
      
      $hdr=pack("v",$cmd);                    // 16bits Command
      $hdr.=pack("v",$subcmd);                // 16bits Subcommand
      $hdr.=pack("V",strlen($this->payload)); // 32bits PayloadSize
      $hdr.=pack("V",RCVERSION);              // 32bits Version
      $hdr.=pack("V",$this->client_id);       // 32bits ClientID      
      $hdr.=pack("V",$this->client_token);    // 32bits ClientToken
      $hdr.=pack("V",$checksum);              // 32bits CheckSum
      $hdr.=pack("V",0);                      // 32bits Reserved1
      $hdr.=pack("V",0);                      // 32bits Reserved2

      $buf=$hdr.$this->payload;
      
      if(RCDEBUG) echo "HDR ".strlen($hdr)." bytes : ".hexstr($hdr)."\n";
      if(RCDEBUG) echo "Payload ".strlen($this->payload)." bytes : ".hexstr($this->payload)."\n";
      if(RCDEBUG) echo "Send ".strlen($buf)." bytes : ".hexstr($buf)."\n";
      socket_write($this->socket,$buf,strlen($buf));
      
      return true;
    }

    // ==================================================================================================
    // (Internal) Get the reply following a command, 
    function getreply()
    {
      if(RCDEBUG) echo "ReplicationRelayClient:getreply\n";
      $reply=socket_read($this->socket,9);
      if($reply==false) die("ReplicationRelayClient:getreply - No Reply : ".socket_strerror(socket_last_error())."\n");
      
      if(RCDEBUG) echo "Received ".strlen($reply)." bytes : ".hexstr($reply)."\n";
      
      $hdr=unpack("Cresult/Vpayloadsize/Vchecksum",$reply);
      
      if(RCDEBUG) echo "=> Result     {$hdr['result']}\n";
      if(RCDEBUG) echo "=> PayloadSize {$hdr['payloadsize']}\n";
      if(RCDEBUG) echo "=> CheckSum    ".sprintf("%04X",$hdr['checksum'])."\n";
      
      if($hdr['payloadsize']>0)
      {
        $this->payload=socket_read($this->socket,$hdr['payloadsize']);
        if(RCDEBUG) echo "Received payload ".strlen($this->payload)." bytes : ".hexstr($this->payload)."\n";
        if($this->payload==false) die("ReplicationRelayClient:getreply/payload - No Reply : ".socket_strerror(socket_last_error())."\n");
      }
      else $this->payload="";
      
      $checksum=$hdr['result']+$hdr['payloadsize'];
      for($i=0;$i<strlen($this->payload);$i++) $checksum+=ord($this->payload[$i]);
      
      if($checksum!=$hdr['checksum'])
      {
        if(RCDEBUG) echo "ReplicationRelayClient:getreply - checksum mismatch $checksum<>{$hdr['checksum']}\n";
        return 0xFE;
      }
      return $hdr['result'];
    }

    // ==================================================================================================
    // Ping the replication relay, returns false upon success    
    function ping()
    {
      if(RCDEBUG) echo "ReplicationRelayClient:ping\n";

      $this->payload="";
      $resu=$this->sendcommand(REPRELAY_PING,0);
      $resu=$this->getreply();
      
      if(RCDEBUG) echo "ReplicationRelayClient:ping - Result : ".sprintf("%02X",$resu)." [{$this->payload}]\n";
      
      if($resu==0) return true;
      else return false;
    }
    
    // ==================================================================================================
    // Connect to the replication relay on host:port, return true if succeeds
    function connect($host,$port)
    {
      if(RCDEBUG) echo "ReplicationRelayClient:connect\n";
      $this->server_host=$host;
      $this->server_port=$port;
      
      $this->socket=socket_create(AF_INET,SOCK_STREAM,SOL_TCP);
      if($this->socket===false) die("ReplicationRelayClient:connect - Failed to create socket : ".socket_strerror(socket_last_error())."\n");

      $resu=socket_connect($this->socket,$this->server_host,$this->server_port);
      if($resu===false) die("ReplicationRelayClient - Failed to connect : ".socket_strerror(socket_last_error())."\n");

      $resu=$this->ping();
      if($resu===false) die("ReplicationRelayClient - PING failed\n");
      
      return true;
    }
        
    // ==================================================================================================
    // After a connect, authenticate to the replication relay (shared secret), return true upon success
    function auth($clientname,$authkey)
    {
      if(RCDEBUG) echo "ReplicationRelayClient:auth($clientname,$authkey)\n";
          
      $this->payload="";
      $this->payload.=pack("Q",$authkey);
      if(RCDEBUG) printf("Auth Key : $authkey / %016X\n ",$authkey);
      $this->payload.=pack("N",0);        // ClientToken
      $this->payload.=pack("N",0);        // ClientID
      $this->payload.=pack("V",strlen($clientname)+1);        // ClientNameSize
      $this->payload.=$clientname."\00";  // Null terminated name

      $resu=$this->sendcommand(REPRELAY_AUTH,0);
      $resu=$this->getreply();
      
      if($resu==0)
      {
        $hdr=unpack("Qauth/Vtoken/Vclientid",$this->payload);
      
        //print_r($hdr);
        $this->client_id=$hdr["clientid"];
        $this->client_token=$hdr["token"];
      }
      
      if($resu==0) return true;
      else return false;
     
    }
    
    // ==================================================================================================  
    // Add a filter to the session with the replication relay (filtertype = REPRELAYREQ_INSERT / REPRELAYREQ_UPDATE / REPRELAYREQ_DELETE), schemaname, tablename, filterdiscardtype (in case of limited queue, how to handle extra events : ignore, delete oldest, delete newest), filterqueuelimit = limit of the queue if > 0)
    function reqfilter($filtertype,$schemaname,$tablename,$filterdiscardtype,$filterqueuelimit)
    {
      if(RCDEBUG) echo "ReplicationRelayClient:auth($filtertype,$schemaname,$tablename,$filterdiscardtype,$filterqueuelimit)\n";
      
      $this->payload="";
      $this->payload.=pack("C",$filtertype);
      $this->payload.=pack("C",$filterdiscardtype);
      $this->payload.=pack("V",$filterqueuelimit);

      $this->payload.=pack("V",strlen($schemaname)+1);        // SchemaNameSize
      $this->payload.=$schemaname."\00";  // Null terminated name
      $this->payload.=pack("V",strlen($tablename)+1);        // TableNameSize
      $this->payload.=$tablename."\00";  // Null terminated name

      $resu=$this->sendcommand(REPRELAY_ADDFILTER,0);
      $resu=$this->getreply();
      
      if($resu==0) return true;
      else return false;      
    }
  
  // ==================================================================================================  
  // Internal fuction
    function popstring($str,&$offset)
    {
      $data=unpack("Vsz",$str,$offset);
      $offset+=4;
      $resu="";
      for($i=0;$i<$data["sz"]-1;$i++) $resu.=$str[$offset++];
      $offset++; // Null term
      return $resu;
    }
  // ==================================================================================================  
  // Poll for a replication event, returns a structure containing the result (component queuesize indicates how many more events are avilable), or NULL if no event available
    function poll()
    {
      $offset=0;
      if(RCDEBUG) echo "ReplicationRelayClient:poll()\n";
      $resu=$this->sendcommand(REPRELAY_POLL,0);
      $resu=$this->getreply();
      if($resu==0)
      {
        $item=new StdClass();
        $item->eventtype=ord($this->payload[$offset++]);
         $data=unpack("Peid/Vqsz",$this->payload,$offset);
        $offset+=12;        
        $item->eid=$data["eid"];
        $item->queuesize=$data["qsz"];
        $item->schema=$this->popstring($this->payload,$offset);
        $item->table=$this->popstring($this->payload,$offset);
        $data=unpack("Vsz",$this->payload,$offset);
        $offset+=4;
        $item->cols=Array();
        for($i=0;$i<$data["sz"];$i++)
        {
          $col=new StdClass();
          $col->name=$this->popstring($this->payload,$offset);          
          $col->before=$this->popstring($this->payload,$offset);
          $col->after=$this->popstring($this->payload,$offset);
          $item->cols[$col->name]=$col;
        }
        return $item;
      }
      else return NULL;      
    }
  // ==================================================================================================  
  
  }
  
?>