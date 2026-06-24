import json
import logging
import multiprocessing
from common import middleware, message_protocol

ANSWER_TIMEOUT = 2
LEADER_TIMEOUT = 5
VOTING = 1
WORKING  = 0
HEARTBEAT_SLEEP = 2
HEARTBEAT_TIMEOUT = 6

ANSWER = "ANSWER"
ELECTION = "ELECTION"
COORDINATOR = "COORDINATOR"
HEARTBEAT = "HEARTBEAT"

class NodeManagerIntercomm:
    def __init__(self, id):
        self.id = id
        self.timer_handle = None
        self.received_answer = False
        self.voting_status = None
        self.voting_status_gate = None
        self.is_leader = None
        self.heartbeat_timer = None

    def start(self, status, status_gate, is_leader,  mom_host):
        logging.basicConfig(level=logging.INFO)
        self.exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            host=mom_host,
            exchange_name="bully_election",
            routing_keys=[f"manager{self.id}", "leader"],
            consumer_id = self.id,
            exchange_type="topic"
        )
        self.voting_status = status
        self.voting_status_gate = status_gate
        self.is_leader = is_leader

        if not self.is_leader.value:
            self._resetear_timeout_lider()

        self.exchange.start_consuming(on_message_callback=self._callback)
    
    def iniciar_votacion(self):
        if self.timer_handle:
            self.exchange.remove_timeout(self.timer_handle)
            
        self.received_answer = False
        self.voting_status.value = VOTING     
        self.voting_status_gate.clear()   
        self.is_leader.value = False
        
        bully = False
        msg=message_protocol.internal.serialize({"type": ELECTION, "sender": self.id})
        
        for target_id in range(0, self.id):
            self.exchange.send_by_key(msg, key=f"manager{target_id}")
            bully = True
            
        if bully:
            self.timer_handle = self.exchange.call_later(
                ANSWER_TIMEOUT, self._timeout_esperar_bully
            )
        else:
            msg = message_protocol.internal.serialize({"type": COORDINATOR, "leader": self.id})
            self.exchange.send_by_key(msg, key="leader")
            self.is_leader.value = True
            self.voting_status.value = WORKING
            self.voting_status_gate.set()
            self._heartbeat()
    
    def _timeout_esperar_bully(self):
        if not self.received_answer:
            # nodo se  declara lider si nadie le responde
            msg = message_protocol.internal.serialize({"type": COORDINATOR, "leader": self.id})
            self.exchange.send_by_key(msg, key="leader")
            self.is_leader.value = True
            self.voting_status.value = WORKING
            self.voting_status_gate.set()
            self._heartbeat()

    def _heartbeat(self):
        if self.is_leader.value:
            try:
                msg = message_protocol.internal.serialize({"type": HEARTBEAT, "sender": self.id})
                self.exchange.send_by_key(msg, key="leader")
                self.heartbeat_timer = self.exchange.call_later(HEARTBEAT_SLEEP, self._heartbeat)
            except Exception as e:
                logging.error(f"{self.id} Error enviando heartbeat: {e}")
    
    def _resetear_timeout_lider(self):
        if self.heartbeat_timer:
            self.exchange.remove_timeout(self.heartbeat_timer)
        
        self.heartbeat_timer = self.exchange.call_later(
            HEARTBEAT_TIMEOUT, self._timeout_lider
        )
    
    def _timeout_lider(self):
        logging.info(f"{self.id} detectó la caída del líder Iniciando bully")
        self.iniciar_votacion()

    def _timeout_lider(self):
        self.iniciar_votacion()

    def _limpiar_timers(self):
        if self.timer_handle:
            self.exchange.remove_timeout(self.timer_handle)
            self.timer_handle = None
        if self.heartbeat_timer:
            self.exchange.remove_timeout(self.heartbeat_timer)
            self.heartbeat_timer = None

    def _callback(self, message,  ack, nack, ctx):
        msg = message_protocol.internal.deserialize(message)
        msg_type = msg["type"]
        
        if msg_type == "HEARTBEAT":
            sender = msg["sender"]
            if not self.is_leader.value and sender != self.id:
                self._resetear_timeout_lider()
        elif msg_type == ELECTION:
            sender = msg["sender"]
            reply =  message_protocol.internal.serialize({"type": ANSWER, "sender": self.id})
            self.exchange.send_by_key(reply, key=f"manager{sender}")
            
            if self.voting_status.value == WORKING:
                self.iniciar_votacion()   
        elif msg_type == ANSWER:
            sender = msg["sender"]
            self.received_answer = True
            self._limpiar_timers()
                
            self.timer_handle = self.exchange.call_later(
                LEADER_TIMEOUT, self._timeout_lider
            )
        elif msg_type == COORDINATOR:
            lider_actual = msg["leader"]
            logging.info(f"Votacion terminada el lider es {lider_actual}")
            
            if lider_actual != self.id:
                self._limpiar_timers()
                    
                self.is_leader.value = False
                self.voting_status.value = WORKING
                self.voting_status_gate.set()
                self._resetear_timeout_lider()
        ack()