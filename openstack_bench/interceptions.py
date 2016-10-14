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


class AnalysisPoint(object):
    def __init__(self,
                 inject_point,
                 project=None):
        self.inject_point = inject_point
        self._release_points = {}
        if not project:
            self.project = inject_point.split(".")[0]
        else:
            self.project = project

    def __getitem__(self, key):
        return self._release_points[key]

    def __setitem__(self, key, item):
        if not isinstance(key, list):
            key = [key]
        for k in key:
            if k in self._release_points:
                raise RuntimeError("AnalysisPoint %s already registered %s!"
                                   % (self.inject_point, k))
            else:
                self._release_points[k] = item

    def __contains__(self, key):
        return key in self._release_points


class ReleasePoint(object):
    def __init__(self, before=None, after=None, excep=None):
        if not before and not after and not excep:
            raise RuntimeError()
        self.before = before
        self.after = after
        self.excep = excep
