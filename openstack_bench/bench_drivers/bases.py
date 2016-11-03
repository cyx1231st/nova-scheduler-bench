# Copyright (c) 2016 Yingxin Cheng
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from openstack_bench import interceptions


class BenchDriverBase(object):
    SERVICES = None

    def __init__(self):
        self.release = None
        self.points = {}
        self.register_points()

    def register_points(self):
        raise NotImplementedError()

    def build_graph(self):
        raise NotImplementedError()

    def build_statistics(self, s_engine, report):
        raise NotImplementedError()

    def register_point(self, place, before=None,
                       after=None, excep=None, release=None):
        point = self.points.get(place)
        r_point = interceptions.ReleasePoint(before, after, excep)
        if not point:
            point = interceptions.AnalysisPoint(place)
            self.points[place] = point
        point[release] = r_point

    def check_service(self, service):
        return service in self.SERVICES
